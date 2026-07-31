use std::path::{Path, PathBuf};
use std::process::Stdio;

use tokio::process::{Child, Command};
use tracing::{info, warn};

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
    child: Option<Child>,
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

    pub async fn start(&mut self, cfg: &ExportConfig) -> anyhow::Result<()> {
        std::fs::write(&self.config_path, cfg.render())?;
        info!(path = %self.config_path.display(), "wrote ganesha config");

        let child = Command::new("ganesha.nfsd")
            .arg("-F")
            .arg("-p")
            .arg("/var/run/ganesha.pid")
            .arg("-f")
            .arg(&self.config_path)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
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
            Some(c) => c.try_wait().ok().flatten().is_none(),
            None => false,
        }
    }

    pub async fn stop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.start_kill();
            match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                c.wait(),
            )
            .await
            {
                Ok(Ok(_)) => info!("ganesha.nfsd stopped"),
                _ => {
                    warn!("ganesha.nfsd didn't stop gracefully, killing");
                    let _ = c.kill().await;
                }
            }
        }
    }
}

impl Drop for Ganesha {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.start_kill();
        }
    }
}

pub fn endpoint(svc_ip: &str, pseudo_path: &str) -> String {
    format!("{svc_ip}:{pseudo_path}")
}
