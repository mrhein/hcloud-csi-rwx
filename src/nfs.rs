use std::path::{Path, PathBuf};

use tracing::{info, warn};

use crate::exec::{args, ProcessHandle, ProcessSpawner};

/// NFS export configuration for a single volume.
pub struct ExportConfig {
    pub export_id: u16,
    pub export_path: String,
    pub pseudo_path: String,
    pub lease_lifetime: u32,
    pub grace_period: u32,
    /// Comma-separated client CIDRs allowed to mount the export.
    /// `*` or `0.0.0.0/0` exports to everyone (requires external firewalling).
    pub allowed_clients: String,
    /// Port for the Prometheus metrics endpoint. `0` disables metrics.
    /// ganesha binds it on `Bind_addr` (0.0.0.0) — with hostNetwork that is
    /// the node IP, so firewall it like :2049.
    pub monitoring_port: u16,
}

impl Default for ExportConfig {
    fn default() -> Self {
        Self {
            export_id: 1,
            export_path: "/export".into(),
            pseudo_path: "/".into(),
            lease_lifetime: 60,
            grace_period: 90,
            allowed_clients: "10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16".into(),
            monitoring_port: 9587,
        }
    }
}

/// Validate a value before interpolating it into the ganesha config.
/// Volume names come from Kubernetes object names, paths are fixed by us —
/// this is a defense-in-depth check against config injection.
pub fn validate_config_value(value: &str) -> anyhow::Result<()> {
    if value.is_empty() {
        anyhow::bail!("empty value not allowed in ganesha config");
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/' | ',' | ' ' | '*' | ':'))
    {
        anyhow::bail!("invalid character in ganesha config value: {value:?}");
    }
    Ok(())
}

impl ExportConfig {
    /// Generate ganesha config based on Longhorn's share-manager config.
    /// Lease_Lifetime and Grace_Period are configurable for failover tuning.
    /// See: longhorn-share-manager/pkg/server/nfs/nfs_server.go
    pub fn render(&self) -> String {
        // With a CLIENT block, unmatched clients fall back to the EXPORT-level
        // Access_Type = None — i.e. the export is only usable from the listed
        // CIDRs. `*` / `0.0.0.0/0` opts out of the restriction.
        let open_export = self.allowed_clients.trim() == "*"
            || self.allowed_clients.trim() == "0.0.0.0/0";
        let (export_access, client_block) = if open_export {
            ("RW".to_string(), String::new())
        } else {
            (
                "None".to_string(),
                format!(
                    r#"    CLIENT {{
        Clients = {clients};
        Access_Type = RW;
        Squash = None;
        SecType = sys;
    }}
"#,
                    clients = self.allowed_clients
                ),
            )
        };

        // Prometheus exposer (compiled in via USE_MONITORING). Enable_Metrics
        // defaults to false upstream, so it only runs when we ask for it.
        let metrics_block = if self.monitoring_port == 0 {
            String::new()
        } else {
            format!(
                "    Enable_Metrics = true;\n    Monitoring_Port = {};\n",
                self.monitoring_port
            )
        };

        format!(
            r#"NFS_Core_Param
{{
    Enable_UDP = false;
    fsid_device = false;
    Bind_addr = 0.0.0.0;
    Protocols = 4;
{metrics_block}}}

LOG {{
    Default_Log_Level = INFO;
    Facility {{
        name = FILE;
        destination = "/proc/1/fd/1";
        enable = active;
    }}
}}

NFSV4
{{
    Lease_Lifetime = {lease_lifetime};
    Grace_Period = {grace_period};
    Minor_Versions = 0, 1, 2;
    RecoveryBackend = hcloud;
    Only_Numeric_Owners = true;
}}

Export_Defaults
{{
    Protocols = 4;
    Transports = TCP;
    Access_Type = None;
    SecType = sys;
    Squash = None;
}}

EXPORT
{{
    Export_Id = {export_id};
    Path = {path};
    Pseudo = {pseudo};
    Protocols = 4;
    Transports = TCP;
    Access_Type = {export_access};
    Squash = None;
    SecType = sys;
    Filesystem_id = {export_id}.0;
{client_block}    FSAL {{
        Name = VFS;
    }}
}}
"#,
            export_id = self.export_id,
            path = self.export_path,
            pseudo = self.pseudo_path,
            lease_lifetime = self.lease_lifetime,
            grace_period = self.grace_period,
        )
    }
}

/// Manages the ganesha daemon process lifecycle.
pub struct Ganesha {
    child: Option<Box<dyn ProcessHandle>>,
    config_path: PathBuf,
}

impl Ganesha {
    pub fn new(config_dir: &str) -> Self {
        std::fs::create_dir_all(config_dir).ok();
        Self {
            child: None,
            config_path: Path::new(config_dir).join("ganesha.conf"),
        }
    }

    /// Path of the config file this instance writes.
    pub fn config_path(&self) -> &Path {
        &self.config_path
    }

    /// Write the config and spawn ganesha.nfsd.
    pub async fn start(
        &mut self,
        spawner: &dyn ProcessSpawner,
        cfg: &ExportConfig,
    ) -> anyhow::Result<()> {
        std::fs::write(&self.config_path, cfg.render())?;
        info!(path = %self.config_path.display(), "wrote ganesha config");

        let child = spawner
            .spawn(
                "ganesha.nfsd",
                &args(&[
                    "-F",
                    "-p",
                    "/var/run/ganesha.pid",
                    "-f",
                    &self.config_path.to_string_lossy(),
                ]),
            )
            .map_err(|e| anyhow::anyhow!("failed to spawn ganesha.nfsd: {e}"))?;

        self.child = Some(child);
        info!(
            lease_lifetime = cfg.lease_lifetime,
            grace_period = cfg.grace_period,
            "ganesha.nfsd started"
        );
        Ok(())
    }

    pub fn is_running(&mut self) -> bool {
        match &mut self.child {
            Some(c) => c.is_running(),
            None => false,
        }
    }

    pub async fn stop(&mut self) {
        if let Some(mut c) = self.child.take() {
            c.start_kill();
            if c.wait_with_timeout(std::time::Duration::from_secs(5)).await {
                info!("ganesha.nfsd stopped");
            } else {
                warn!("ganesha.nfsd didn't stop gracefully");
            }
        }
    }
}

impl Drop for Ganesha {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            c.start_kill();
        }
    }
}

pub fn endpoint(svc_ip: &str, pseudo_path: &str) -> String {
    format!("{svc_ip}:{pseudo_path}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::FakeExec;

    fn tmpdir(name: &str) -> String {
        let d = std::env::temp_dir().join(name);
        let _ = std::fs::remove_dir_all(&d);
        d.to_string_lossy().into_owned()
    }

    #[test]
    fn endpoint_joins_ip_and_pseudo() {
        assert_eq!(endpoint("10.0.0.1", "/vol"), "10.0.0.1:/vol");
    }

    #[test]
    fn config_value_validation() {
        assert!(validate_config_value("pvc-1234").is_ok());
        assert!(validate_config_value("10.0.0.0/8, 192.168.0.0/16").is_ok());
        assert!(validate_config_value("*").is_ok());
        assert!(validate_config_value("").is_err());
        assert!(validate_config_value("a;\nEXPORT {").is_err());
    }

    #[test]
    fn render_open_export_has_no_client_block() {
        for open in ["*", "0.0.0.0/0"] {
            let cfg = ExportConfig { allowed_clients: open.into(), ..Default::default() };
            let r = cfg.render();
            assert!(!r.contains("CLIENT {"), "{open} should not restrict clients");
            assert!(r.contains("Access_Type = RW;"));
        }
    }

    #[test]
    fn render_metrics_toggle() {
        let on = ExportConfig::default().render();
        assert!(on.contains("Enable_Metrics = true;"));
        assert!(on.contains("Monitoring_Port = 9587;"));

        let off = ExportConfig { monitoring_port: 0, ..Default::default() }.render();
        assert!(!off.contains("Enable_Metrics"));
        assert!(!off.contains("Monitoring_Port"));
    }

    #[test]
    fn render_interpolates_all_fields() {
        let cfg = ExportConfig {
            export_id: 7,
            export_path: "/export/x".into(),
            pseudo_path: "/x".into(),
            lease_lifetime: 30,
            grace_period: 45,
            allowed_clients: "10.1.0.0/16".into(),
            monitoring_port: 1234,
        };
        let r = cfg.render();
        for needle in [
            "Export_Id = 7;",
            "Path = /export/x;",
            "Pseudo = /x;",
            "Lease_Lifetime = 30;",
            "Grace_Period = 45;",
            "Filesystem_id = 7.0;",
            "Clients = 10.1.0.0/16;",
            "Monitoring_Port = 1234;",
            "RecoveryBackend = hcloud;",
            "Name = VFS;",
        ] {
            assert!(r.contains(needle), "missing {needle} in:\n{r}");
        }
    }

    #[tokio::test]
    async fn ganesha_start_writes_config_and_spawns() {
        let dir = tmpdir("hcloud-ganesha-start");
        let mut g = Ganesha::new(&dir);
        let fake = FakeExec::new();
        g.start(&fake, &ExportConfig::default()).await.unwrap();

        let written = std::fs::read_to_string(g.config_path()).unwrap();
        assert!(written.contains("EXPORT"));
        assert!(fake.ran("ganesha.nfsd -F -p /var/run/ganesha.pid -f"));
        assert!(g.is_running());

        g.stop().await;
        assert!(!g.is_running(), "no child after stop");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn ganesha_reports_spawn_failure() {
        let dir = tmpdir("hcloud-ganesha-fail");
        let mut g = Ganesha::new(&dir);
        let fake = FakeExec::new().failing_spawn();
        let err = g.start(&fake, &ExportConfig::default()).await.unwrap_err();
        assert!(err.to_string().contains("failed to spawn ganesha.nfsd"));
        assert!(!g.is_running());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn ganesha_detects_dead_process() {
        let dir = tmpdir("hcloud-ganesha-dead");
        let mut g = Ganesha::new(&dir);
        let fake = FakeExec::new().alive_for(1);
        g.start(&fake, &ExportConfig::default()).await.unwrap();
        assert!(g.is_running());
        assert!(!g.is_running(), "process exited on the second poll");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn ganesha_stop_without_start_is_noop() {
        let dir = tmpdir("hcloud-ganesha-noop");
        let mut g = Ganesha::new(&dir);
        g.stop().await;
        assert!(!g.is_running());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn ganesha_drop_kills_child() {
        let dir = tmpdir("hcloud-ganesha-drop");
        let fake = FakeExec::new();
        {
            let mut g = Ganesha::new(&dir);
            g.start(&fake, &ExportConfig::default()).await.unwrap();
        } // Drop runs here
        assert!(fake.ran("ganesha.nfsd"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
