//! hcloud-csi-rwx — RWX (ReadWriteMany) volumes for Hetzner Cloud CSI.
//!
//! The binaries in `src/bin` are thin shells around these modules so the
//! logic can be unit-tested without a cluster:
//!
//! - [`sharemanager`] — mounts the block volume and runs ganesha
//! - [`csi`] — the CSI gRPC driver (provisioning + node mount)
//! - [`controller`] — the failover controller
//! - [`recovery`] — the NFSv4 recovery backend HTTP API
//!
//! [`exec`] abstracts every external command, and [`testing`] provides a
//! mock Kubernetes API, so both the process and the API boundary are fakeable.

pub mod api;
pub mod block;
pub mod controller;
pub mod csi;
pub mod exec;
pub mod nfs;
pub mod provision;
pub mod recovery;
pub mod sharemanager;

#[cfg(test)]
pub mod testing;

/// Generated from the upstream CSI spec (proto/csi.proto).
pub mod csi_proto {
    #![allow(clippy::doc_overindented_list_items)]
    #![allow(clippy::doc_lazy_continuation)]
    #![allow(clippy::enum_variant_names)]
    tonic::include_proto!("csi.v1");
}

/// Initialise tracing from `RUST_LOG`, defaulting to `info`.
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .try_init();
}

#[cfg(test)]
mod tests {
    #[test]
    fn tracing_init_is_idempotent() {
        // A second call must not panic — binaries and tests may both call it.
        super::init_tracing();
        super::init_tracing();
    }
}
