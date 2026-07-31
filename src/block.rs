//! Block device handling for the share-manager: wait for the device, create a
//! filesystem if needed, mount it, and probe its health.
//!
//! Everything that shells out goes through [`CommandRunner`] so the logic is
//! testable without a privileged container.

use std::path::Path;

use nix::unistd::Uid;
use tracing::{info, warn};

use crate::exec::{args, CommandRunner};

/// Wait for a block device to appear, returning its canonical path.
///
/// The CSI driver attaches the hcloud volume asynchronously, so the device
/// node shows up some time after the pod starts. This is the same readiness
/// check the Longhorn share-manager uses.
pub async fn wait_for_device(
    dev_path: &str,
    timeout: std::time::Duration,
) -> anyhow::Result<String> {
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
/// Uses `mkfs.ext4` — Longhorn also defaults to ext4 unless the user selects
/// xfs via storage class.
pub async fn ensure_filesystem(runner: &dyn CommandRunner, device: &str) -> anyhow::Result<()> {
    // Quick probe: if blkid already reports a filesystem we skip creation.
    let probe = runner
        .run("blkid", &args(&["-c", "/dev/null", device]), None)
        .await;
    if matches!(&probe, Ok(o) if o.success) {
        info!(device, "filesystem already present, skipping mkfs");
        return Ok(());
    }

    info!(device, "creating ext4 filesystem");
    let out = runner
        .run("mkfs.ext4", &args(&["-F", device]), None)
        .await
        .map_err(|e| anyhow::anyhow!("failed to run mkfs.ext4: {e}"))?;
    if !out.success {
        anyhow::bail!("mkfs.ext4 exited with code {:?}", out.code);
    }
    Ok(())
}

/// Mount the block device at `mount_point`, creating the directory first.
pub async fn mount_device(
    runner: &dyn CommandRunner,
    device: &str,
    mount_point: &str,
) -> anyhow::Result<()> {
    std::fs::create_dir_all(mount_point)?;
    let out = runner
        .run("mount", &args(&["-t", "ext4", device, mount_point]), None)
        .await
        .map_err(|e| anyhow::anyhow!("failed to run mount: {e}"))?;
    if !out.success {
        anyhow::bail!("mount {device} at {mount_point} exited with code {:?}", out.code);
    }
    info!(device, mount_point, "volume mounted");
    Ok(())
}

/// Unmount if currently mounted. Idempotent — tolerates "not mounted".
pub async fn unmount_device(runner: &dyn CommandRunner, mount_point: &str) -> anyhow::Result<()> {
    let out = runner
        .run("umount", &args(&[mount_point]), None)
        .await
        .map_err(|e| anyhow::anyhow!("failed to run umount: {e}"))?;
    if out.success {
        info!(mount_point, "unmounted");
        return Ok(());
    }
    // Already gone is not an error.
    if !Path::new(mount_point).exists() {
        return Ok(());
    }
    warn!(mount_point, "umount returned non-zero, may already be unmounted");
    Ok(())
}

/// Apply permissions to the export root so NFS clients (Squash = None) can
/// write. Best-effort: a failure here is logged, not fatal.
pub async fn set_export_mode(runner: &dyn CommandRunner, mount_point: &str, mode: &str) {
    match runner.run("chmod", &args(&[mode, mount_point]), None).await {
        Ok(o) if o.success => info!(mount_point, mode, "set export permissions"),
        Ok(o) => warn!(mount_point, mode, code = ?o.code, "chmod returned non-zero"),
        Err(e) => warn!(mount_point, mode, error = %e, "chmod failed to run"),
    }
}

/// Run a filesystem health check — write + remove a probe file.
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

/// Validate an octal permission string like `0777`.
pub fn valid_export_mode(mode: &str) -> bool {
    (3..=4).contains(&mode.len()) && mode.chars().all(|c| ('0'..='7').contains(&c))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::{FakeExec, Outcome};

    #[tokio::test]
    async fn existing_filesystem_skips_mkfs() {
        let fake = FakeExec::new(); // blkid succeeds by default
        ensure_filesystem(&fake, "/dev/sdb").await.unwrap();
        assert!(fake.ran("blkid -c /dev/null /dev/sdb"));
        assert!(!fake.ran("mkfs.ext4"), "must not reformat an existing fs");
    }

    #[tokio::test]
    async fn missing_filesystem_triggers_mkfs() {
        let fake = FakeExec::new().on("blkid", Outcome::failed(2));
        ensure_filesystem(&fake, "/dev/sdb").await.unwrap();
        assert!(fake.ran("mkfs.ext4 -F /dev/sdb"));
    }

    #[tokio::test]
    async fn mkfs_failure_is_reported() {
        let fake = FakeExec::new()
            .on("blkid", Outcome::failed(2))
            .on("mkfs.ext4", Outcome::failed(1));
        let err = ensure_filesystem(&fake, "/dev/sdb").await.unwrap_err();
        assert!(err.to_string().contains("mkfs.ext4 exited"));
    }

    #[tokio::test]
    async fn mkfs_spawn_error_is_reported() {
        let fake = FakeExec::new()
            .on("blkid", Outcome::failed(2))
            .on_error("mkfs.ext4");
        let err = ensure_filesystem(&fake, "/dev/sdb").await.unwrap_err();
        assert!(err.to_string().contains("failed to run mkfs.ext4"));
    }

    #[tokio::test]
    async fn mount_uses_ext4_and_reports_failure() {
        let dir = std::env::temp_dir().join("hcloud-csi-rwx-mount-test");
        let mp = dir.to_str().unwrap();

        let fake = FakeExec::new();
        mount_device(&fake, "/dev/sdb", mp).await.unwrap();
        assert!(fake.ran(&format!("mount -t ext4 /dev/sdb {mp}")));

        let bad = FakeExec::new().on("mount", Outcome::failed(32));
        let err = mount_device(&bad, "/dev/sdb", mp).await.unwrap_err();
        assert!(err.to_string().contains("exited with code"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn unmount_tolerates_missing_mountpoint() {
        let fake = FakeExec::new().on("umount", Outcome::failed(1));
        // path does not exist -> treated as already unmounted
        unmount_device(&fake, "/definitely/not/mounted/anywhere").await.unwrap();
        assert!(fake.ran("umount"));
    }

    #[tokio::test]
    async fn unmount_warns_when_path_still_exists() {
        let fake = FakeExec::new().on("umount", Outcome::failed(1));
        unmount_device(&fake, "/tmp").await.unwrap();
        assert!(fake.ran("umount /tmp"));
    }

    #[tokio::test]
    async fn unmount_success_path() {
        let fake = FakeExec::new();
        unmount_device(&fake, "/tmp").await.unwrap();
    }

    #[tokio::test]
    async fn unmount_spawn_error_is_reported() {
        let fake = FakeExec::new().on_error("umount");
        assert!(unmount_device(&fake, "/tmp").await.is_err());
    }

    #[tokio::test]
    async fn set_export_mode_covers_all_branches() {
        let ok = FakeExec::new();
        set_export_mode(&ok, "/export", "0777").await;
        assert!(ok.ran("chmod 0777 /export"));

        let nonzero = FakeExec::new().on("chmod", Outcome::failed(1));
        set_export_mode(&nonzero, "/export", "0777").await;

        let broken = FakeExec::new().on_error("chmod");
        set_export_mode(&broken, "/export", "0777").await;
    }

    #[tokio::test]
    async fn health_check_roundtrip() {
        let dir = std::env::temp_dir().join("hcloud-csi-rwx-health-test");
        std::fs::create_dir_all(&dir).unwrap();
        assert!(health_check(dir.to_str().unwrap()).await);
        assert!(!health_check("/no/such/dir/at/all").await);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn wait_for_device_finds_and_times_out() {
        let f = std::env::temp_dir().join("hcloud-csi-rwx-dev-test");
        std::fs::write(&f, b"x").unwrap();
        let got = wait_for_device(f.to_str().unwrap(), std::time::Duration::from_secs(1))
            .await
            .unwrap();
        assert!(got.contains("hcloud-csi-rwx-dev-test"));
        std::fs::remove_file(&f).unwrap();

        let err = wait_for_device("/dev/nonexistent-hcloud-test", std::time::Duration::from_millis(50))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("timeout waiting for device"));
    }

    #[test]
    fn export_mode_validation() {
        assert!(valid_export_mode("0777"));
        assert!(valid_export_mode("755"));
        assert!(!valid_export_mode("0778"));
        assert!(!valid_export_mode("77"));
        assert!(!valid_export_mode("07777"));
        assert!(!valid_export_mode("rwx"));
    }

    #[test]
    fn check_root_is_callable() {
        // Value depends on the test environment; we only assert it runs.
        let _ = check_root();
    }
}
