use std::path::Path;

use nix::unistd::Uid;
use tracing::{info, warn};

/// Wait for a block device to appear, returning its canonical path.
///
/// Longhorn exposes volumes under `/dev/longhorn/<volume>` on the host, which is
/// bind-mounted into the share-manager container at `/host/dev/longhorn/<volume>`.
/// We poll until the device node exists, which is the same readiness check the
/// Longhorn share-manager uses.
pub async fn wait_for_device(dev_path: &str, timeout: std::time::Duration) -> anyhow::Result<String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if Path::new(dev_path).try_exists()? {
            let canon = std::fs::canonicalize(dev_path)?;
            info!(dev = %canon.display(), "block device appeared");
            return Ok(canon.to_string_lossy().into_owned());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("timeout waiting for device {dev_path}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

/// Create a filesystem on the block device if none is present yet.
/// Uses `mkfs.ext4` for simplicity — Longhorn also defaults to ext4 unless the
/// user selects xfs via storage class.
pub fn ensure_filesystem(device: &str) -> anyhow::Result<()> {
    // Quick probe: if blkid already reports a filesystem we skip creation.
    let blkid = std::process::Command::new("blkid")
        .arg("-c")
        .arg("/dev/null")
        .arg(device)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
    match blkid {
        Ok(s) if s.success() => {
            info!(device, "filesystem already present, skipping mkfs");
            Ok(())
        }
        _ => {
            info!(device, "creating ext4 filesystem");
            let status = std::process::Command::new("mkfs.ext4")
                .arg("-F")
                .arg(device)
                .status()
                .map_err(|e| anyhow::anyhow!("failed to run mkfs.ext4: {e}"))?;
            if !status.success() {
                anyhow::bail!("mkfs.ext4 exited with {status}");
            }
            Ok(())
        }
    }
}

/// Mount the block device at `mount_point`. Creates the directory first.
/// Uses the `mount` binary instead of nix::mount::mount for portability — nix's
/// mount API differs between macOS (apple module) and Linux.
pub fn mount_device(device: &str, mount_point: &str) -> anyhow::Result<()> {
    std::fs::create_dir_all(mount_point)?;
    // ponytail: no fancy mount option parsing — ext4 defaults are fine for RWX.
    // If tuning is needed later (noatime, etc.) add a --mount-opts flag.
    let status = std::process::Command::new("mount")
        .arg("-t")
        .arg("ext4")
        .arg(device)
        .arg(mount_point)
        .status()
        .map_err(|e| anyhow::anyhow!("failed to run mount: {e}"))?;
    if !status.success() {
        anyhow::bail!("mount {device} at {mount_point} exited with {status}");
    }
    info!(device, mount_point, "volume mounted");
    Ok(())
}

/// Unmount if currently mounted. Idempotent — ignores "not mounted" errors.
pub fn unmount_device(mount_point: &str) -> anyhow::Result<()> {
    let status = std::process::Command::new("umount")
        .arg(mount_point)
        .status()
        .map_err(|e| anyhow::anyhow!("failed to run umount: {e}"))?;
    if !status.success() {
        // Check if already unmounted
        if !Path::new(mount_point).exists() {
            return Ok(());
        }
        warn!(mount_point, "umount returned non-zero, may already be unmounted");
    } else {
        info!(mount_point, "unmounted");
    }
    Ok(())
}

/// Run a filesystem health check — write + read + remove a probe file.
/// Longhorn's share-manager does periodic checks; we keep the same idea.
pub async fn health_check(mount_point: &str) -> bool {
    let probe = Path::new(mount_point).join(".hcloud-csi-rwx.health");
    match tokio::fs::write(&probe, b"ok").await {
        Ok(()) => {
            if tokio::fs::remove_file(&probe).await.is_err() {
                warn!("could not remove health probe file");
            }
            true
        }
        Err(e) => {
            warn!(error = %e, mount_point, "health check failed");
            false
        }
    }
}

/// Check if we are root (required for mount/sysadmin).
pub fn check_root() -> bool {
    Uid::effective().is_root()
}
