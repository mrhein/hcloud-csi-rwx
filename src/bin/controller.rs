//! Thin shell — the logic lives in `hcloud_csi_rwx::controller`.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    hcloud_csi_rwx::init_tracing();
    hcloud_csi_rwx::controller::main().await
}
