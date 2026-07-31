//! hcloud-csi-rwx share-manager — CLI shell.
//!
//! Mirrors Longhorn's share-manager pattern: take an attached block volume,
//! mount it, export it via NFS (ganesha), and expose a small HTTP API so the
//! CSI driver can discover the NFS endpoint and check readiness.
//!
//! All logic lives in `hcloud_csi_rwx::sharemanager`; this file only parses
//! arguments and wires up the real process/command implementations.

use std::sync::Arc;

use clap::{Parser, Subcommand};
use tracing::{error, info};

use hcloud_csi_rwx::api;
use hcloud_csi_rwx::block;
use hcloud_csi_rwx::exec::SystemExec;
use hcloud_csi_rwx::sharemanager::{self, Settings};

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
        /// Block device path, e.g. /dev/sdX.
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
        /// IP clients use to reach the NFS export. Under hostNetwork this is
        /// the pod IP == node IP (injected via the downward API).
        #[arg(long, env = "NFS_SVC_IP")]
        svc_ip: String,
        /// Directory for ganesha config + pid files.
        #[arg(long, env = "GANESHA_CONFIG_DIR", default_value = "/var/run/ganesha")]
        ganesha_dir: String,
        /// Timeout waiting for the block device to appear (seconds).
        #[arg(long, env = "DEVICE_TIMEOUT", default_value_t = 120)]
        device_timeout: u64,
        /// Skip block device detection, mkfs, and mount — the volume is
        /// already mounted by CSI at --mount-point. Only start ganesha.
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
        /// Port for ganesha's Prometheus metrics endpoint (0 = disabled).
        /// Bound on the node IP under hostNetwork — firewall it.
        #[arg(long, env = "MONITORING_PORT", default_value_t = 9587)]
        monitoring_port: u16,
    },
}

impl Command {
    /// Convert the `Run` variant into [`Settings`].
    fn into_settings(self) -> Option<(Settings, String)> {
        match self {
            Command::Run {
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
                monitoring_port,
            } => {
                let listen = api_listen.clone();
                Some((
                    Settings {
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
                        monitoring_port,
                    },
                    listen,
                ))
            }
            _ => None,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    hcloud_csi_rwx::init_tracing();

    match Cli::parse().command {
        Some(Command::PrintConfig { volume, mount_point, export_id }) => {
            print!("{}", sharemanager::print_config(&volume, &mount_point, export_id));
            Ok(())
        }
        None => anyhow::bail!("no subcommand given — use `run` (see --help)"),
        Some(cmd) => {
            let (settings, listen) = cmd.into_settings().expect("only Run remains");
            info!(device = %settings.device, volume = %settings.volume, "starting share-manager");

            if !block::check_root() {
                error!("must run as root (need mount/sysadmin capabilities)");
                std::process::exit(1);
            }

            let state = api::new_state(&settings.volume);

            // Start the HTTP API early so the CSI driver sees "not ready"
            // while we boot.
            let api_state = state.clone();
            let api_task = tokio::spawn(async move {
                if let Err(e) = sharemanager::serve_api(&listen, api_state).await {
                    error!(error = %e, "share-manager API failed");
                    std::process::exit(1);
                }
            });

            let exec = SystemExec;
            let shutdown = Box::pin(async {
                let _ = tokio::signal::ctrl_c().await;
            });

            let result = sharemanager::run(
                &exec,
                &exec,
                &settings,
                Arc::clone(&state),
                shutdown,
                std::time::Duration::from_secs(10),
            )
            .await;

            api_task.abort();

            // Exit non-zero on health failures so the pod ends up in phase
            // Failed and the failover controller reacts immediately.
            if let Err(ref e) = result {
                error!(error = %e, "share-manager failed");
            }
            match sharemanager::exit_code(&result) {
                0 => Ok(()),
                code => std::process::exit(code),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn run_args_map_to_settings() {
        let cli = Cli::parse_from([
            "hcloud-csi-rwx",
            "run",
            "--device",
            "/dev/sdb",
            "--volume",
            "pvc-1",
            "--svc-ip",
            "10.0.0.7",
            "--skip-mount",
            "--monitoring-port",
            "1234",
        ]);
        let (s, listen) = cli.command.unwrap().into_settings().unwrap();
        assert_eq!(s.device, "/dev/sdb");
        assert_eq!(s.volume, "pvc-1");
        assert_eq!(s.svc_ip, "10.0.0.7");
        assert!(s.skip_mount);
        assert_eq!(s.monitoring_port, 1234);
        assert_eq!(s.mount_point, "/export", "default applies");
        assert_eq!(listen, "0.0.0.0:9500");
        s.validate().unwrap();
    }

    #[test]
    fn print_config_variant_has_no_settings() {
        let cli = Cli::parse_from([
            "hcloud-csi-rwx",
            "print-config",
            "--volume",
            "v",
        ]);
        assert!(cli.command.unwrap().into_settings().is_none());
    }

    #[test]
    fn no_subcommand_parses_to_none() {
        let cli = Cli::parse_from(["hcloud-csi-rwx"]);
        assert!(cli.command.is_none());
    }

    #[test]
    fn cli_definition_is_valid() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }
}
