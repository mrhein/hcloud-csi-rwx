mod api;
mod block;
mod nfs;

use std::sync::Arc;

use clap::{Parser, Subcommand};
use tokio::sync::RwLock;
use tracing::{error, info};

use api::ShareState;
use nfs::{ExportConfig, Ganesha};

/// hcloud-csi-rwx share-manager.
///
/// Mirrors Longhorn's share-manager pattern: take an attached block volume,
/// mount it, export it via NFS (ganesha), and expose a small HTTP API so the
/// CSI driver can discover the NFS endpoint and check readiness.
#[derive(Parser, Debug)]
#[command(name = "hcloud-csi-rwx", about = "RWX share-manager for hcloud block volumes")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Print the ganesha config for a given export without starting anything.
    PrintConfig {
        #[arg(long, env = "VOLUME_NAME")]
        volume: String,
        #[arg(long, env = "VOLUME_MOUNT", default_value = "/export")]
        mount_point: String,
        #[arg(long, default_value_t = 1)]
        export_id: u16,
    },
    /// Start the share-manager daemon.
    Run {
        /// Block device path, e.g. /dev/longhorn/pvc-xxxx or /dev/sdX.
        #[arg(long, env = "VOLUME_DEVICE")]
        device: String,
        /// Volume name used as export pseudo path and in the API state.
        #[arg(long, env = "VOLUME_NAME")]
        volume: String,
        /// Where to mount the block device locally.
        #[arg(long, env = "VOLUME_MOUNT", default_value = "/export")]
        mount_point: String,
        /// HTTP API listen address (health + endpoint discovery).
        #[arg(long, env = "API_LISTEN", default_value = "0.0.0.0:9500")]
        api_listen: String,
        /// Service cluster IP that clients use to reach the NFS export.
        /// The CSI NodePublishVolume will mount `<svc_ip>:/<pseudo>`.
        #[arg(long, env = "NFS_SVC_IP")]
        svc_ip: String,
        /// Directory for ganesha config + pid files.
        #[arg(long, env = "GANESHA_CONFIG_DIR", default_value = "/var/run/ganesha")]
        ganesha_dir: String,
        /// Timeout waiting for the block device to appear (seconds).
        #[arg(long, env = "DEVICE_TIMEOUT", default_value_t = 120)]
        device_timeout: u64,
        /// Skip block device detection, mkfs, and mount — the volume is already
        /// mounted by CSI at --mount-point. Only start ganesha.
        #[arg(long, env = "SKIP_MOUNT", default_value_t = false)]
        skip_mount: bool,
        /// NFSv4 lease lifetime in seconds (Longhorn default: 60).
        #[arg(long, env = "LEASE_LIFETIME", default_value_t = 60)]
        lease_lifetime: u32,
        /// NFSv4 grace period in seconds (Longhorn default: 90).
        #[arg(long, env = "GRACE_PERIOD", default_value_t = 90)]
        grace_period: u32,
        /// Comma-separated CIDRs allowed to mount the export.
        /// `*` exports to everyone (requires external firewalling of :2049).
        #[arg(
            long,
            env = "NFS_ALLOWED_CLIENTS",
            default_value = "10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16"
        )]
        allowed_clients: String,
        /// Permissions applied to the export root so NFS clients can write.
        #[arg(long, env = "EXPORT_MODE", default_value = "0777")]
        export_mode: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Some(Command::PrintConfig {
            volume,
            mount_point,
            export_id,
        }) => {
            let pseudo = format!("/{volume}");
            let cfg = ExportConfig {
                export_id,
                export_path: mount_point,
                pseudo_path: pseudo,
                ..ExportConfig::default()
            };
            print!("{}", cfg.render());
            Ok(())
        }
        None => {
            anyhow::bail!("no subcommand given — use `run` (see --help)");
        }
        Some(Command::Run {
            device,
            volume,
            mount_point,
            api_listen,
            svc_ip,
            ganesha_dir,
            device_timeout,
            skip_mount,
            lease_lifetime,
            grace_period,
            allowed_clients,
            export_mode,
        }) => {
            info!(device = %device, volume = %volume, "starting share-manager");

            if !block::check_root() {
                error!("must run as root (need mount/sysadmin capabilities)");
                std::process::exit(1);
            }

            // Everything interpolated into the ganesha config gets validated.
            nfs::validate_config_value(&volume)?;
            nfs::validate_config_value(&mount_point)?;
            nfs::validate_config_value(&allowed_clients)?;
            if !(3..=4).contains(&export_mode.len())
                || !export_mode.chars().all(|c| ('0'..='7').contains(&c))
            {
                anyhow::bail!("invalid --export-mode {export_mode:?}, expected octal like 0777");
            }

            let state = api::new_state(&volume);

            // Start HTTP API early so the CSI driver can see "not ready" while we boot.
            let api_state = state.clone();
            let api_addr = api_listen.clone();
            let api_task = tokio::spawn(async move {
                let listener = match tokio::net::TcpListener::bind(&api_addr).await {
                    Ok(l) => l,
                    Err(e) => {
                        error!(addr = %api_addr, error = %e, "failed to bind API");
                        std::process::exit(1);
                    }
                };
                info!(addr = %api_addr, "API listening");
                if let Err(e) = axum::serve(listener, api::app(api_state)).await {
                    error!(error = %e, "API server error");
                    std::process::exit(1);
                }
            });

            let dev = if skip_mount {
                info!(mount_point = %mount_point, "skip-mount mode: assuming CSI already mounted volume");
                mount_point.clone()
            } else {
                match block::wait_for_device(
                    &device,
                    std::time::Duration::from_secs(device_timeout),
                )
                .await
                {
                    Ok(d) => d,
                    Err(e) => {
                        error!(error = %e, "failed to find block device");
                        set_error(&state, e.to_string()).await;
                        std::process::exit(1);
                    }
                }
            };

            if !skip_mount {
                if let Err(e) = block::ensure_filesystem(&dev) {
                    error!(error = %e, "filesystem creation failed");
                    set_error(&state, e.to_string()).await;
                    std::process::exit(1);
                }
                if let Err(e) = block::mount_device(&dev, &mount_point) {
                    error!(error = %e, "mount failed");
                    set_error(&state, e.to_string()).await;
                    std::process::exit(1);
                }
            }

            // The volume is mounted as root; open it up so NFS clients
            // (Squash = None) can write. Mode is configurable via EXPORT_MODE.
            let _ = std::process::Command::new("chmod")
                .arg(&export_mode)
                .arg(&mount_point)
                .status();
            info!(mount_point = %mount_point, mode = %export_mode, "set export permissions");

            let pseudo = format!("/{volume}");
            let export_cfg = ExportConfig {
                export_id: 1,
                export_path: mount_point.clone(),
                pseudo_path: pseudo.clone(),
                lease_lifetime,
                grace_period,
                allowed_clients,
            };

            let mut ganesha = Ganesha::new(&ganesha_dir);
            if let Err(e) = ganesha.start(&export_cfg).await {
                error!(error = %e, "ganesha start failed");
                set_error(&state, e.to_string()).await;
                let _ = block::unmount_device(&mount_point);
                std::process::exit(1);
            }

            let endpoint = nfs::endpoint(&svc_ip, &pseudo);
            {
                let mut s = state.write().await;
                s.ready = true;
                s.endpoint = Some(endpoint.clone());
                s.error = None;
            }
            info!(endpoint = %endpoint, "share-manager ready");

            let mut health_interval = tokio::time::interval(std::time::Duration::from_secs(10));
            health_interval.tick().await;

            // Exit non-zero on health failures so the pod ends up in phase
            // Failed and the failover controller reacts immediately.
            let mut healthy_shutdown = false;
            loop {
                tokio::select! {
                    _ = health_interval.tick() => {
                        if !ganesha.is_running() {
                            error!("ganesha process died");
                            mark_unhealthy(&state, "ganesha process exited").await;
                            break;
                        }
                        if !block::health_check(&mount_point).await {
                            error!("volume health check failed");
                            mark_unhealthy(&state, "volume health check failed").await;
                            break;
                        }
                    }
                    _ = tokio::signal::ctrl_c() => {
                        info!("received SIGINT, shutting down");
                        healthy_shutdown = true;
                        break;
                    }
                }
            }

            ganesha.stop().await;
            if !skip_mount {
                let _ = block::unmount_device(&mount_point);
            }
            api_task.abort();
            if !healthy_shutdown {
                std::process::exit(1);
            }
            Ok(())
        }
    }
}

async fn set_error(state: &Arc<RwLock<ShareState>>, msg: String) {
    let mut s = state.write().await;
    s.ready = false;
    s.error = Some(msg);
}

async fn mark_unhealthy(state: &Arc<RwLock<ShareState>>, msg: &str) {
    let mut s = state.write().await;
    s.ready = false;
    s.error = Some(msg.to_string());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ganesha_config_contains_export() {
        let cfg = ExportConfig {
            export_id: 42,
            export_path: "/export/pvc-test".into(),
            pseudo_path: "/pvc-test".into(),
            ..ExportConfig::default()
        };
        let rendered = cfg.render();
        assert!(rendered.contains("Export_Id = 42;"));
        assert!(rendered.contains("Path = /export/pvc-test;"));
        assert!(rendered.contains("Pseudo = /pvc-test;"));
        assert!(rendered.contains("Access_Type = RW;"));
        assert!(rendered.contains("Name = VFS;"));
        assert!(rendered.contains("Protocols = 4;"));
        // default: restricted to private networks via CLIENT block
        assert!(rendered.contains("CLIENT {"));
        assert!(rendered.contains("Clients = 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16;"));
    }

    #[test]
    fn ganesha_config_open_export_without_client_block() {
        let cfg = ExportConfig {
            allowed_clients: "*".into(),
            ..ExportConfig::default()
        };
        let rendered = cfg.render();
        assert!(!rendered.contains("CLIENT {"));
        assert!(rendered.contains("Access_Type = RW;"));
    }

    #[test]
    fn config_value_validation() {
        assert!(nfs::validate_config_value("pvc-1234-abcd").is_ok());
        assert!(nfs::validate_config_value("10.0.0.0/8, 192.168.0.0/16").is_ok());
        assert!(nfs::validate_config_value("*").is_ok());
        assert!(nfs::validate_config_value("").is_err());
        assert!(nfs::validate_config_value("foo;\nEXPORT {").is_err());
        assert!(nfs::validate_config_value("foo\"bar").is_err());
    }

    #[test]
    fn endpoint_format() {
        assert_eq!(
            nfs::endpoint("10.43.0.5", "/pvc-test"),
            "10.43.0.5:/pvc-test"
        );
    }

    #[test]
    fn share_state_serializes() {
        let st = ShareState {
            ready: true,
            endpoint: Some("1.2.3.4:/vol".into()),
            volume: "vol".into(),
            error: None,
        };
        let json = serde_json::to_string(&st).unwrap();
        assert!(json.contains("\"ready\":true"));
        assert!(json.contains("\"endpoint\":\"1.2.3.4:/vol\""));
    }
}

#[cfg(test)]
mod api_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn healthz_returns_503_when_not_ready() {
        let state = api::new_state("test-vol");
        let app = api::app(state);
        let resp = app
            .oneshot(Request::builder().uri("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn endpoint_returns_200_when_ready() {
        let state = api::new_state("test-vol");
        {
            let mut s = state.write().await;
            s.ready = true;
            s.endpoint = Some("10.0.0.1:/test-vol".into());
        }
        let app = api::app(state);
        let resp = app
            .oneshot(Request::builder().uri("/endpoint").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["ready"], true);
        assert_eq!(json["endpoint"], "10.0.0.1:/test-vol");
    }

    #[tokio::test]
    async fn state_endpoint_returns_volume_name() {
        let state = api::new_state("my-pvc");
        let app = api::app(state);
        let resp = app
            .oneshot(Request::builder().uri("/state").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["volume"], "my-pvc");
    }
}
