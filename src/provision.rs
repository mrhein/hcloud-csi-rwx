//! Shared provisioning helpers used by both the CSI driver (`bin/csi.rs`) and
//! the failover controller (`bin/controller.rs`).
//!
//! Keeping the share-manager pod/service specs and the configuration lookups
//! in one place guarantees the two binaries never diverge.

use k8s_openapi::api::core::v1::{Node, Pod};
use kube::api::{Api, ListParams};
use kube::runtime::reflector::Lookup;
use kube::Client;
use serde_json::{json, Value};

/// CSI driver name, storage class name, and app label value.
pub const DRIVER_NAME: &str = "hcloud-csi-rwx";
/// Name of the optional Secret holding the recovery-backend bearer token
/// (key: `token`). If absent, recovery-backend auth is disabled.
pub const RECOVERY_TOKEN_SECRET: &str = "hcloud-csi-rwx-recovery-token";

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

/// Namespace where share-manager pods, backing PVCs, and state live.
pub fn share_namespace() -> String {
    env_or("SHARE_MANAGER_NAMESPACE", "hcloud-csi-rwx")
}

/// Container image for share-manager pods.
pub fn image() -> String {
    env_or(
        "SHARE_MANAGER_IMAGE",
        concat!("ghcr.io/mrhein/hcloud-csi-rwx:v", env!("CARGO_PKG_VERSION")),
    )
}

/// imagePullPolicy for share-manager pods.
pub fn pull_policy() -> String {
    env_or("SHARE_MANAGER_PULL_POLICY", "IfNotPresent")
}

/// StorageClass used for the backing RWO block volume.
pub fn backing_storage_class() -> String {
    env_or("BACKING_STORAGE_CLASS", "hcloud-volumes")
}

/// CIDRs allowed to mount the NFS exports. `*` disables the restriction.
pub fn nfs_allowed_clients() -> String {
    env_or(
        "NFS_ALLOWED_CLIENTS",
        "10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16",
    )
}

/// NFSv4 lease lifetime / grace period (seconds), forwarded to share-managers.
pub fn lease_lifetime() -> String {
    env_or("LEASE_LIFETIME", "60")
}

pub fn grace_period() -> String {
    env_or("GRACE_PERIOD", "90")
}

/// Permissions applied to the export root so NFS clients can write.
pub fn export_mode() -> String {
    env_or("EXPORT_MODE", "0777")
}

/// Sanitize a volume/PVC name for use in resource names.
pub fn volume_name(pvc: &str) -> String {
    pvc.replace(['.', '_'], "-")
}

pub fn backing_pvc_name(vn: &str) -> String {
    format!("{vn}-backing")
}

pub fn share_pod_name(vn: &str) -> String {
    format!("share-manager-{vn}")
}

pub fn state_cm_name(vn: &str) -> String {
    format!("state-{vn}")
}

/// JSON spec for the backing RWO PVC.
pub fn backing_pvc_json(volume: &str, size: &str, backing_sc: &str) -> Value {
    let vn = volume_name(volume);
    json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": backing_pvc_name(&vn),
            "namespace": share_namespace(),
            "labels": { "app": DRIVER_NAME, "rwx-volume": volume }
        },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "storageClassName": backing_sc,
            "volumeMode": "Filesystem",
            "resources": { "requests": { "storage": size } }
        }
    })
}

/// JSON spec for the per-volume ClusterIP service (NFS + share-manager API).
pub fn service_json(volume: &str) -> Value {
    let vn = volume_name(volume);
    json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": vn,
            "namespace": share_namespace(),
            "labels": { "app": DRIVER_NAME, "rwx-volume": volume }
        },
        "spec": {
            "type": "ClusterIP",
            "selector": { "app": DRIVER_NAME, "rwx-volume": volume },
            "ports": [
                { "port": 2049, "name": "nfs", "targetPort": 2049 },
                { "port": 9500, "name": "api", "targetPort": 9500 }
            ]
        }
    })
}

/// JSON spec for the share-manager pod. `pvc_name`/`pvc_namespace` are the
/// RWX claim this volume serves (used by the failover controller to evict
/// workload pods); they may be empty when unknown.
pub fn share_pod_json(volume: &str, node: &str, pvc_name: &str, pvc_namespace: &str) -> Value {
    let vn = volume_name(volume);
    let mut env = vec![
        json!({ "name": "LEASE_LIFETIME", "value": lease_lifetime() }),
        json!({ "name": "GRACE_PERIOD", "value": grace_period() }),
        json!({ "name": "NFS_ALLOWED_CLIENTS", "value": nfs_allowed_clients() }),
        json!({ "name": "EXPORT_MODE", "value": export_mode() }),
        json!({ "name": "RECOVERY_BACKEND_TOKEN", "valueFrom": {
            "secretKeyRef": {
                "name": RECOVERY_TOKEN_SECRET,
                "key": "token",
                "optional": true
            }
        }}),
    ];
    if let Ok(url) = std::env::var("HCLOUD_RECOVERY_BACKEND_URL")
        && !url.is_empty() {
            env.push(json!({ "name": "HCLOUD_RECOVERY_BACKEND_URL", "value": url }));
        }

    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": share_pod_name(&vn),
            "namespace": share_namespace(),
            "labels": {
                "app": DRIVER_NAME,
                "rwx-volume": volume,
                "rwx-pvc-name": pvc_name,
                "rwx-pvc-namespace": pvc_namespace
            }
        },
        "spec": {
            "hostNetwork": true,
            "dnsPolicy": "ClusterFirstWithHostNet",
            "nodeSelector": { "kubernetes.io/hostname": node },
            "restartPolicy": "Never",
            "containers": [{
                "name": "share-manager",
                "image": image(),
                "imagePullPolicy": pull_policy(),
                "securityContext": { "privileged": true },
                "args": [
                    "run",
                    "--device", "/export",
                    "--volume", volume,
                    "--mount-point", "/export",
                    "--svc-ip", vn,
                    "--api-listen", "0.0.0.0:9500",
                    "--skip-mount"
                ],
                "env": env,
                "ports": [
                    { "containerPort": 9500, "name": "api" },
                    { "containerPort": 2049, "name": "nfs" }
                ],
                "volumeMounts": [{
                    "name": "block-dev",
                    "mountPath": "/export",
                    "mountPropagation": "Bidirectional"
                }]
            }],
            "volumes": [{
                "name": "block-dev",
                "persistentVolumeClaim": { "claimName": backing_pvc_name(&vn) }
            }]
        }
    })
}

/// Pick a Ready node that is not in `avoid` and does not already host a
/// share-manager pod (ganesha binds :2049 on the host, so only one
/// share-manager fits per node).
pub async fn pick_node(client: &Client, avoid: &[String]) -> anyhow::Result<String> {
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), &share_namespace());
    let occupied: Vec<String> = pod_api
        .list(&ListParams::default().labels("rwx-volume"))
        .await?
        .into_iter()
        .filter_map(|p| p.spec.as_ref().and_then(|s| s.node_name.clone()))
        .collect();

    let node_api: Api<Node> = Api::all(client.clone());
    let nodes = node_api.list(&ListParams::default()).await?;
    nodes
        .into_iter()
        .filter(|n| {
            let name = n.name().unwrap_or_default().to_string();
            let ready = n
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .map(|c| {
                    c.iter()
                        .any(|cond| cond.type_ == "Ready" && cond.status == "True")
                })
                .unwrap_or(false);
            ready && !avoid.contains(&name) && !occupied.contains(&name)
        })
        .map(|n| n.name().unwrap_or_default().to_string())
        .next()
        .ok_or_else(|| anyhow::anyhow!("no suitable node found (avoid={avoid:?})"))
}

/// True if the given node is Ready.
pub async fn node_is_ready(client: &Client, node: &str) -> bool {
    let node_api: Api<Node> = Api::all(client.clone());
    match node_api.get(node).await {
        Ok(n) => n
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .map(|c| {
                c.iter()
                    .any(|cond| cond.type_ == "Ready" && cond.status == "True")
            })
            .unwrap_or(false),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volume_name_sanitizes() {
        assert_eq!(volume_name("pvc-1234"), "pvc-1234");
        assert_eq!(volume_name("my.vol_1"), "my-vol-1");
    }

    #[test]
    fn share_pod_has_consistent_labels_and_claim() {
        let pod = share_pod_json("pvc-abc", "node-1", "my-claim", "default");
        assert_eq!(pod["metadata"]["labels"]["app"], DRIVER_NAME);
        assert_eq!(pod["metadata"]["labels"]["rwx-volume"], "pvc-abc");
        assert_eq!(pod["metadata"]["labels"]["rwx-pvc-name"], "my-claim");
        assert_eq!(
            pod["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"],
            "pvc-abc-backing"
        );
        assert_eq!(
            pod["spec"]["nodeSelector"]["kubernetes.io/hostname"],
            "node-1"
        );
        // service selector must match pod labels
        let svc = service_json("pvc-abc");
        assert_eq!(svc["spec"]["selector"]["app"], DRIVER_NAME);
        assert_eq!(svc["spec"]["selector"]["rwx-volume"], "pvc-abc");
    }

    #[test]
    fn image_default_matches_crate_version() {
        // No env override in tests: default must point at the released image.
        if std::env::var("SHARE_MANAGER_IMAGE").is_err() {
            assert!(image().starts_with("ghcr.io/mrhein/hcloud-csi-rwx:v"));
        }
    }
}
