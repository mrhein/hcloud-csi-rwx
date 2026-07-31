//! The share-manager: take an attached block volume, mount it, export it via
//! NFSv4 (ganesha), and expose a small HTTP API so the CSI driver can discover
//! the NFS endpoint and check readiness.
//!
//! Split into small units so the startup sequence and the health loop can be
//! tested without root, a block device, or a real ganesha.

use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{error, info};

use crate::api::ShareState;
use crate::block;
use crate::exec::{CommandRunner, ProcessSpawner};
use crate::nfs::{self, ExportConfig, Ganesha};

/// Everything the share-manager needs to run one export.
#[derive(Debug, Clone)]
pub struct Settings {
    pub device: String,
    pub volume: String,
    pub mount_point: String,
    pub api_listen: String,
    pub svc_ip: String,
    pub ganesha_dir: String,
    pub device_timeout: u64,
    pub skip_mount: bool,
    pub lease_lifetime: u32,
    pub grace_period: u32,
    pub allowed_clients: String,
    pub export_mode: String,
    pub monitoring_port: u16,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            device: "/export".into(),
            volume: "vol".into(),
            mount_point: "/export".into(),
            api_listen: "0.0.0.0:9500".into(),
            svc_ip: "10.0.0.1".into(),
            ganesha_dir: "/var/run/ganesha".into(),
            device_timeout: 120,
            skip_mount: true,
            lease_lifetime: 60,
            grace_period: 90,
            allowed_clients: "10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16".into(),
            export_mode: "0777".into(),
            monitoring_port: 9587,
        }
    }
}

impl Settings {
    /// Reject anything that would be interpolated unescaped into the ganesha
    /// config, plus the octal export mode.
    pub fn validate(&self) -> anyhow::Result<()> {
        nfs::validate_config_value(&self.volume)?;
        nfs::validate_config_value(&self.mount_point)?;
        nfs::validate_config_value(&self.allowed_clients)?;
        if !block::valid_export_mode(&self.export_mode) {
            anyhow::bail!(
                "invalid --export-mode {:?}, expected octal like 0777",
                self.export_mode
            );
        }
        Ok(())
    }

    /// NFSv4 pseudo path of this export.
    pub fn pseudo_path(&self) -> String {
        format!("/{}", self.volume)
    }

    /// The ganesha export configuration derived from these settings.
    pub fn export_config(&self) -> ExportConfig {
        ExportConfig {
            export_id: 1,
            export_path: self.mount_point.clone(),
            pseudo_path: self.pseudo_path(),
            lease_lifetime: self.lease_lifetime,
            grace_period: self.grace_period,
            allowed_clients: self.allowed_clients.clone(),
            monitoring_port: self.monitoring_port,
        }
    }

    /// Endpoint clients mount, e.g. `10.0.0.1:/pvc-123`.
    pub fn endpoint(&self) -> String {
        nfs::endpoint(&self.svc_ip, &self.pseudo_path())
    }
}

/// Why the health loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    /// SIGINT — an orderly shutdown, exit code 0.
    Signal,
    /// ganesha died; the pod must reach phase `Failed` so the failover
    /// controller reacts, so this exits non-zero.
    GaneshaDied,
    /// The volume stopped being writable.
    VolumeUnhealthy,
}

impl ExitReason {
    /// Whether this was a clean shutdown.
    pub fn is_clean(self) -> bool {
        matches!(self, ExitReason::Signal)
    }

    pub fn message(self) -> &'static str {
        match self {
            ExitReason::Signal => "received SIGINT, shutting down",
            ExitReason::GaneshaDied => "ganesha process exited",
            ExitReason::VolumeUnhealthy => "volume health check failed",
        }
    }
}

pub async fn set_error(state: &Arc<RwLock<ShareState>>, msg: String) {
    let mut s = state.write().await;
    s.ready = false;
    s.error = Some(msg);
}

pub async fn mark_unhealthy(state: &Arc<RwLock<ShareState>>, msg: &str) {
    let mut s = state.write().await;
    s.ready = false;
    s.error = Some(msg.to_string());
}

pub async fn mark_ready(state: &Arc<RwLock<ShareState>>, endpoint: &str) {
    let mut s = state.write().await;
    s.ready = true;
    s.endpoint = Some(endpoint.to_string());
    s.error = None;
}

/// Prepare the volume: wait for the device, create a filesystem and mount it
/// (unless CSI already did), then open up the export root. Returns the device
/// path actually used.
pub async fn prepare_volume(
    runner: &dyn CommandRunner,
    settings: &Settings,
    state: &Arc<RwLock<ShareState>>,
) -> anyhow::Result<String> {
    let dev = if settings.skip_mount {
        info!(mount_point = %settings.mount_point, "skip-mount mode: assuming CSI already mounted volume");
        settings.mount_point.clone()
    } else {
        let d = block::wait_for_device(
            &settings.device,
            std::time::Duration::from_secs(settings.device_timeout),
        )
        .await
        .inspect_err(|e| error!(error = %e, "failed to find block device"))?;

        block::ensure_filesystem(runner, &d)
            .await
            .inspect_err(|e| error!(error = %e, "filesystem creation failed"))?;
        block::mount_device(runner, &d, &settings.mount_point)
            .await
            .inspect_err(|e| error!(error = %e, "mount failed"))?;
        d
    };

    // The volume is mounted as root; open it up so NFS clients (Squash = None)
    // can write.
    block::set_export_mode(runner, &settings.mount_point, &settings.export_mode).await;

    let _ = state; // state is updated by the caller on failure
    Ok(dev)
}

/// One iteration of the health loop. `None` means "still healthy".
pub async fn health_tick(ganesha: &mut Ganesha, mount_point: &str) -> Option<ExitReason> {
    if !ganesha.is_running() {
        error!("ganesha process died");
        return Some(ExitReason::GaneshaDied);
    }
    if !block::health_check(mount_point).await {
        error!("volume health check failed");
        return Some(ExitReason::VolumeUnhealthy);
    }
    None
}

/// Run the health loop until something ends it.
pub async fn health_loop(
    ganesha: &mut Ganesha,
    state: &Arc<RwLock<ShareState>>,
    mount_point: &str,
    interval: std::time::Duration,
    mut shutdown: impl Future<Output = ()> + Unpin,
) -> ExitReason {
    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await; // the first tick completes immediately

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Some(reason) = health_tick(ganesha, mount_point).await {
                    mark_unhealthy(state, reason.message()).await;
                    return reason;
                }
            }
            _ = &mut shutdown => {
                info!("{}", ExitReason::Signal.message());
                return ExitReason::Signal;
            }
        }
    }
}

/// Render the ganesha config for the `print-config` subcommand.
pub fn print_config(volume: &str, mount_point: &str, export_id: u16) -> String {
    ExportConfig {
        export_id,
        export_path: mount_point.to_string(),
        pseudo_path: format!("/{volume}"),
        ..ExportConfig::default()
    }
    .render()
}

/// Bind and serve the share-manager HTTP API (health + endpoint discovery).
/// Runs until the server stops; returns an error if binding or serving fails.
pub async fn serve_api(listen: &str, state: crate::api::SharedState) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|e| anyhow::anyhow!("failed to bind API on {listen}: {e}"))?;
    serve_api_on(listener, state).await
}

/// Serve the API on an already-bound listener.
pub async fn serve_api_on(
    listener: tokio::net::TcpListener,
    state: crate::api::SharedState,
) -> anyhow::Result<()> {
    info!(addr = ?listener.local_addr().ok(), "API listening");
    axum::serve(listener, crate::api::app(state))
        .await
        .map_err(|e| anyhow::anyhow!("API server error: {e}"))
}

/// Process exit code for a finished run: non-zero on anything but a clean
/// shutdown, so the pod reaches phase `Failed` and failover kicks in.
pub fn exit_code(result: &anyhow::Result<ExitReason>) -> i32 {
    match result {
        Ok(r) if r.is_clean() => 0,
        _ => 1,
    }
}

/// Full share-manager startup + health loop. Returns how it ended.
pub async fn run(
    runner: &dyn CommandRunner,
    spawner: &dyn ProcessSpawner,
    settings: &Settings,
    state: Arc<RwLock<ShareState>>,
    shutdown: impl Future<Output = ()> + Unpin,
    health_interval: std::time::Duration,
) -> anyhow::Result<ExitReason> {
    settings.validate()?;

    if let Err(e) = prepare_volume(runner, settings, &state).await {
        set_error(&state, e.to_string()).await;
        return Err(e);
    }

    let mut ganesha = Ganesha::new(&settings.ganesha_dir);
    if let Err(e) = ganesha.start(spawner, &settings.export_config()).await {
        error!(error = %e, "ganesha start failed");
        set_error(&state, e.to_string()).await;
        if !settings.skip_mount {
            let _ = block::unmount_device(runner, &settings.mount_point).await;
        }
        return Err(e);
    }

    let endpoint = settings.endpoint();
    mark_ready(&state, &endpoint).await;
    info!(endpoint = %endpoint, "share-manager ready");

    let reason = health_loop(
        &mut ganesha,
        &state,
        &settings.mount_point,
        health_interval,
        shutdown,
    )
    .await;

    ganesha.stop().await;
    if !settings.skip_mount {
        let _ = block::unmount_device(runner, &settings.mount_point).await;
    }
    Ok(reason)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api;
    use crate::exec::{FakeExec, Outcome};
    use std::future::pending;

    fn settings(dir: &str) -> Settings {
        Settings {
            ganesha_dir: dir.into(),
            mount_point: std::env::temp_dir().to_string_lossy().into_owned(),
            ..Default::default()
        }
    }

    fn tmpdir(name: &str) -> String {
        let d = std::env::temp_dir().join(name);
        let _ = std::fs::remove_dir_all(&d);
        d.to_string_lossy().into_owned()
    }

    #[test]
    fn validation_rejects_injection_and_bad_mode() {
        let ok = Settings::default();
        ok.validate().unwrap();

        let bad_vol = Settings { volume: "a;\nEXPORT {".into(), ..Default::default() };
        assert!(bad_vol.validate().is_err());

        let bad_mode = Settings { export_mode: "0999".into(), ..Default::default() };
        let err = bad_mode.validate().unwrap_err();
        assert!(err.to_string().contains("invalid --export-mode"));

        let bad_clients = Settings { allowed_clients: "\"x\"".into(), ..Default::default() };
        assert!(bad_clients.validate().is_err());

        let bad_mp = Settings { mount_point: String::new(), ..Default::default() };
        assert!(bad_mp.validate().is_err());
    }

    #[test]
    fn derived_values() {
        let s = Settings { volume: "pvc-1".into(), svc_ip: "10.1.2.3".into(), ..Default::default() };
        assert_eq!(s.pseudo_path(), "/pvc-1");
        assert_eq!(s.endpoint(), "10.1.2.3:/pvc-1");
        let cfg = s.export_config();
        assert_eq!(cfg.pseudo_path, "/pvc-1");
        assert_eq!(cfg.export_id, 1);
        assert_eq!(cfg.monitoring_port, 9587);
    }

    #[test]
    fn exit_reason_semantics() {
        assert!(ExitReason::Signal.is_clean());
        assert!(!ExitReason::GaneshaDied.is_clean());
        assert!(!ExitReason::VolumeUnhealthy.is_clean());
        assert!(ExitReason::GaneshaDied.message().contains("ganesha"));
        assert!(ExitReason::VolumeUnhealthy.message().contains("health"));
        assert!(ExitReason::Signal.message().contains("SIGINT"));
    }

    #[tokio::test]
    async fn state_transitions() {
        let st = api::new_state("v");
        set_error(&st, "boom".into()).await;
        assert!(!st.read().await.ready);
        assert_eq!(st.read().await.error.as_deref(), Some("boom"));

        mark_ready(&st, "1.2.3.4:/v").await;
        assert!(st.read().await.ready);
        assert!(st.read().await.error.is_none());
        assert_eq!(st.read().await.endpoint.as_deref(), Some("1.2.3.4:/v"));

        mark_unhealthy(&st, "dead").await;
        assert!(!st.read().await.ready);
        assert_eq!(st.read().await.error.as_deref(), Some("dead"));
    }

    #[tokio::test]
    async fn prepare_volume_skip_mount_only_chmods() {
        let st = api::new_state("v");
        let fake = FakeExec::new();
        let s = settings(&tmpdir("sm-prep-skip"));
        let dev = prepare_volume(&fake, &s, &st).await.unwrap();
        assert_eq!(dev, s.mount_point);
        assert!(fake.ran("chmod 0777"));
        assert!(!fake.ran("mkfs"), "skip-mount must not format");
        assert!(!fake.ran("mount -t ext4"));
    }

    #[tokio::test]
    async fn prepare_volume_full_path_formats_and_mounts() {
        let st = api::new_state("v");
        let devfile = std::env::temp_dir().join("sm-prep-dev");
        std::fs::write(&devfile, b"x").unwrap();

        let fake = FakeExec::new().on("blkid", Outcome::failed(2));
        let s = Settings {
            skip_mount: false,
            device: devfile.to_string_lossy().into_owned(),
            ..settings(&tmpdir("sm-prep-full"))
        };
        prepare_volume(&fake, &s, &st).await.unwrap();
        assert!(fake.ran("mkfs.ext4"));
        assert!(fake.ran("mount -t ext4"));
        assert!(fake.ran("chmod"));
        std::fs::remove_file(&devfile).unwrap();
    }

    #[tokio::test]
    async fn prepare_volume_reports_missing_device() {
        let st = api::new_state("v");
        let fake = FakeExec::new();
        let s = Settings {
            skip_mount: false,
            device: "/dev/definitely-missing-xyz".into(),
            device_timeout: 0,
            ..settings(&tmpdir("sm-prep-missing"))
        };
        assert!(prepare_volume(&fake, &s, &st).await.is_err());
    }

    #[tokio::test]
    async fn health_tick_detects_dead_ganesha() {
        let dir = tmpdir("sm-tick-dead");
        let mut g = Ganesha::new(&dir);
        let fake = FakeExec::new().alive_for(0);
        g.start(&fake, &ExportConfig::default()).await.unwrap();
        assert_eq!(
            health_tick(&mut g, &std::env::temp_dir().to_string_lossy()).await,
            Some(ExitReason::GaneshaDied)
        );
    }

    #[tokio::test]
    async fn health_tick_detects_unwritable_volume() {
        let dir = tmpdir("sm-tick-vol");
        let mut g = Ganesha::new(&dir);
        let fake = FakeExec::new();
        g.start(&fake, &ExportConfig::default()).await.unwrap();
        assert_eq!(
            health_tick(&mut g, "/no/such/mount/point").await,
            Some(ExitReason::VolumeUnhealthy)
        );
    }

    #[tokio::test]
    async fn health_tick_healthy_returns_none() {
        let dir = tmpdir("sm-tick-ok");
        let mut g = Ganesha::new(&dir);
        let fake = FakeExec::new();
        g.start(&fake, &ExportConfig::default()).await.unwrap();
        assert_eq!(
            health_tick(&mut g, &std::env::temp_dir().to_string_lossy()).await,
            None
        );
    }

    #[tokio::test]
    async fn health_loop_exits_on_shutdown_signal() {
        let dir = tmpdir("sm-loop-signal");
        let mut g = Ganesha::new(&dir);
        let fake = FakeExec::new();
        g.start(&fake, &ExportConfig::default()).await.unwrap();
        let st = api::new_state("v");

        let reason = health_loop(
            &mut g,
            &st,
            &std::env::temp_dir().to_string_lossy(),
            std::time::Duration::from_secs(60),
            Box::pin(async {}),
        )
        .await;
        assert_eq!(reason, ExitReason::Signal);
    }

    #[tokio::test]
    async fn health_loop_exits_when_ganesha_dies() {
        let dir = tmpdir("sm-loop-dead");
        let mut g = Ganesha::new(&dir);
        let fake = FakeExec::new().alive_for(1);
        g.start(&fake, &ExportConfig::default()).await.unwrap();
        let st = api::new_state("v");

        let reason = health_loop(
            &mut g,
            &st,
            &std::env::temp_dir().to_string_lossy(),
            std::time::Duration::from_millis(5),
            Box::pin(pending::<()>()),
        )
        .await;
        assert_eq!(reason, ExitReason::GaneshaDied);
        assert!(!st.read().await.ready);
    }

    #[tokio::test]
    async fn run_happy_path_until_signal() {
        let fake = FakeExec::new();
        let s = settings(&tmpdir("sm-run-ok"));
        let st = api::new_state(&s.volume);

        let reason = run(
            &fake,
            &fake,
            &s,
            st.clone(),
            Box::pin(async {}),
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap();

        assert_eq!(reason, ExitReason::Signal);
        assert!(reason.is_clean());
        assert!(fake.ran("ganesha.nfsd"));
        assert_eq!(st.read().await.endpoint.as_deref(), Some("10.0.0.1:/vol"));
    }

    #[tokio::test]
    async fn run_rejects_invalid_settings() {
        let fake = FakeExec::new();
        let s = Settings { export_mode: "zzz".into(), ..settings(&tmpdir("sm-run-bad")) };
        let st = api::new_state("v");
        assert!(run(&fake, &fake, &s, st, Box::pin(async {}), std::time::Duration::from_secs(1))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn run_reports_ganesha_start_failure() {
        let fake = FakeExec::new().failing_spawn();
        let s = settings(&tmpdir("sm-run-spawnfail"));
        let st = api::new_state("v");
        let err = run(&fake, &fake, &s, st.clone(), Box::pin(async {}), std::time::Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("ganesha"));
        assert!(!st.read().await.ready);
        assert!(st.read().await.error.is_some());
    }

    #[tokio::test]
    async fn run_unmounts_on_failure_when_it_mounted() {
        let devfile = std::env::temp_dir().join("sm-run-dev");
        std::fs::write(&devfile, b"x").unwrap();
        let fake = FakeExec::new().failing_spawn();
        let s = Settings {
            skip_mount: false,
            device: devfile.to_string_lossy().into_owned(),
            ..settings(&tmpdir("sm-run-unmount"))
        };
        let st = api::new_state("v");
        assert!(run(&fake, &fake, &s, st, Box::pin(async {}), std::time::Duration::from_secs(1))
            .await
            .is_err());
        assert!(fake.ran("umount"), "must clean up its own mount");
        std::fs::remove_file(&devfile).unwrap();
    }

    #[tokio::test]
    async fn run_propagates_prepare_failure() {
        let fake = FakeExec::new();
        let s = Settings {
            skip_mount: false,
            device: "/dev/missing-xyz".into(),
            device_timeout: 0,
            ..settings(&tmpdir("sm-run-prepfail"))
        };
        let st = api::new_state("v");
        assert!(run(&fake, &fake, &s, st.clone(), Box::pin(async {}), std::time::Duration::from_secs(1))
            .await
            .is_err());
        assert!(st.read().await.error.is_some());
    }
}

#[cfg(test)]
mod entrypoint_tests {
    use super::*;
    use crate::api;

    #[test]
    fn print_config_renders_the_export() {
        let out = print_config("pvc-9", "/export", 3);
        assert!(out.contains("Export_Id = 3;"));
        assert!(out.contains("Pseudo = /pvc-9;"));
        assert!(out.contains("Path = /export;"));
    }

    #[test]
    fn exit_code_is_zero_only_for_a_clean_shutdown() {
        assert_eq!(exit_code(&Ok(ExitReason::Signal)), 0);
        assert_eq!(exit_code(&Ok(ExitReason::GaneshaDied)), 1);
        assert_eq!(exit_code(&Ok(ExitReason::VolumeUnhealthy)), 1);
        assert_eq!(exit_code(&Err(anyhow::anyhow!("boom"))), 1);
    }

    #[tokio::test]
    async fn serve_api_reports_bind_failures() {
        let err = serve_api("256.256.256.256:1", api::new_state("v")).await.unwrap_err();
        assert!(err.to_string().contains("failed to bind API"), "{err}");
    }

    #[tokio::test]
    async fn serve_api_answers_health_requests() {
        let state = api::new_state("v");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let srv_state = state.clone();
        let handle = tokio::spawn(async move {
            let _ = serve_api_on(listener, srv_state).await;
        });

        let resp = reqwest::get(format!("http://{addr}/healthz")).await.unwrap();
        assert_eq!(resp.status(), 503, "not ready yet");

        mark_ready(&state, "1.2.3.4:/v").await;
        let ok = reqwest::get(format!("http://{addr}/endpoint")).await.unwrap();
        assert_eq!(ok.status(), 200);
        let body: serde_json::Value = ok.json().await.unwrap();
        assert_eq!(body["endpoint"], "1.2.3.4:/v");

        handle.abort();
    }
}
