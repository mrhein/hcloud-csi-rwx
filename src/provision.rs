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

/// Port for ganesha's Prometheus metrics endpoint ("0" disables it).
pub fn monitoring_port() -> String {
    env_or("MONITORING_PORT", "9587")
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
        // hostNetwork: pod IP == node IP — the share-manager reports this as
        // its NFS endpoint (mounts go directly to the node, no kube-proxy).
        json!({ "name": "NFS_SVC_IP", "valueFrom": { "fieldRef": { "fieldPath": "status.podIP" } } }),
        json!({ "name": "LEASE_LIFETIME", "value": lease_lifetime() }),
        json!({ "name": "GRACE_PERIOD", "value": grace_period() }),
        json!({ "name": "NFS_ALLOWED_CLIENTS", "value": nfs_allowed_clients() }),
        json!({ "name": "EXPORT_MODE", "value": export_mode() }),
        json!({ "name": "MONITORING_PORT", "value": monitoring_port() }),
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

    let mp = monitoring_port();
    let metrics_on = mp != "0";
    let mut ports = vec![
        json!({ "containerPort": 9500, "name": "api" }),
        json!({ "containerPort": 2049, "name": "nfs" }),
    ];
    let mut annotations = serde_json::Map::new();
    if let (true, Ok(p)) = (metrics_on, mp.parse::<i64>()) {
        ports.push(json!({ "containerPort": p, "name": "metrics" }));
        annotations.insert("prometheus.io/scrape".into(), json!("true"));
        annotations.insert("prometheus.io/port".into(), json!(mp.clone()));
        annotations.insert("prometheus.io/path".into(), json!("/metrics"));
    }

    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": share_pod_name(&vn),
            "namespace": share_namespace(),
            "annotations": annotations,
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
                    "--api-listen", "0.0.0.0:9500",
                    "--skip-mount"
                ],
                "env": env,
                "ports": ports,
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
        // NFS endpoint must come from the pod/node IP (downward API), and the
        // service name must NOT be passed as --svc-ip.
        let env = pod["spec"]["containers"][0]["env"].as_array().unwrap();
        assert!(env.iter().any(|e| e["name"] == "NFS_SVC_IP"
            && e["valueFrom"]["fieldRef"]["fieldPath"] == "status.podIP"));
        let args = pod["spec"]["containers"][0]["args"].as_array().unwrap();
        assert!(!args.iter().any(|a| a == "--svc-ip"));
    }

    #[test]
    fn share_pod_exposes_metrics_by_default() {
        if std::env::var("MONITORING_PORT").is_ok() {
            return; // env override active, defaults not under test
        }
        let pod = share_pod_json("pvc-abc", "node-1", "my-claim", "default");
        let ports = pod["spec"]["containers"][0]["ports"].as_array().unwrap();
        assert!(ports.iter().any(|p| p["name"] == "metrics" && p["containerPort"] == 9587));
        assert_eq!(pod["metadata"]["annotations"]["prometheus.io/scrape"], "true");
        assert_eq!(pod["metadata"]["annotations"]["prometheus.io/port"], "9587");
        let env = pod["spec"]["containers"][0]["env"].as_array().unwrap();
        assert!(env.iter().any(|e| e["name"] == "MONITORING_PORT" && e["value"] == "9587"));
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

#[cfg(test)]
mod kube_tests {
    use super::*;
    use crate::testing::{empty_list, node_list, pod_list, FakeApi};
    use serde_json::json;

    fn sm_pod(name: &str, node: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": "hcloud-csi-rwx",
                         "labels": {"app": "hcloud-csi-rwx", "rwx-volume": "v"}},
            "spec": {"nodeName": node},
            "status": {"phase": "Running"}
        })
    }

    #[tokio::test]
    async fn pick_node_returns_first_ready_and_free_node() {
        let fake = FakeApi::new()
            .ok("GET /api/v1/namespaces/hcloud-csi-rwx/pods", empty_list("PodList"))
            .ok("GET /api/v1/nodes", node_list(&[("n1", true), ("n2", true)]));
        let node = pick_node(&fake.client(), &[]).await.unwrap();
        assert_eq!(node, "n1");
    }

    #[tokio::test]
    async fn pick_node_skips_not_ready_nodes() {
        let fake = FakeApi::new()
            .ok("GET /api/v1/namespaces/hcloud-csi-rwx/pods", empty_list("PodList"))
            .ok("GET /api/v1/nodes", node_list(&[("n1", false), ("n2", true)]));
        assert_eq!(pick_node(&fake.client(), &[]).await.unwrap(), "n2");
    }

    #[tokio::test]
    async fn pick_node_honours_avoid_list() {
        let fake = FakeApi::new()
            .ok("GET /api/v1/namespaces/hcloud-csi-rwx/pods", empty_list("PodList"))
            .ok("GET /api/v1/nodes", node_list(&[("n1", true), ("n2", true)]));
        let node = pick_node(&fake.client(), &["n1".into()]).await.unwrap();
        assert_eq!(node, "n2");
    }

    #[tokio::test]
    async fn pick_node_skips_nodes_that_already_host_a_share_manager() {
        // ganesha binds :2049 on the host, so one share-manager per node.
        let fake = FakeApi::new()
            .ok(
                "GET /api/v1/namespaces/hcloud-csi-rwx/pods",
                pod_list(vec![sm_pod("share-manager-a", "n1")]),
            )
            .ok("GET /api/v1/nodes", node_list(&[("n1", true), ("n2", true)]));
        assert_eq!(pick_node(&fake.client(), &[]).await.unwrap(), "n2");
    }

    #[tokio::test]
    async fn pick_node_errors_when_nothing_is_left() {
        let fake = FakeApi::new()
            .ok("GET /api/v1/namespaces/hcloud-csi-rwx/pods", empty_list("PodList"))
            .ok("GET /api/v1/nodes", node_list(&[("n1", true)]));
        let err = pick_node(&fake.client(), &["n1".into()]).await.unwrap_err();
        assert!(err.to_string().contains("no suitable node"), "{err}");
    }

    #[tokio::test]
    async fn pick_node_propagates_api_errors() {
        let fake = FakeApi::new(); // everything 404s
        assert!(pick_node(&fake.client(), &[]).await.is_err());
    }

    #[tokio::test]
    async fn node_is_ready_reflects_condition() {
        let ready = FakeApi::new()
            .ok("GET /api/v1/nodes/n1", crate::testing::node("n1", true));
        assert!(node_is_ready(&ready.client(), "n1").await);

        let not_ready = FakeApi::new()
            .ok("GET /api/v1/nodes/n1", crate::testing::node("n1", false));
        assert!(!node_is_ready(&not_ready.client(), "n1").await);
    }

    #[tokio::test]
    async fn node_is_ready_false_when_node_is_gone_or_conditionless() {
        let missing = FakeApi::new();
        assert!(!node_is_ready(&missing.client(), "gone").await);

        let bare = FakeApi::new().ok(
            "GET /api/v1/nodes/n1",
            json!({"apiVersion": "v1", "kind": "Node", "metadata": {"name": "n1"}}),
        );
        assert!(!node_is_ready(&bare.client(), "n1").await);
    }

    #[test]
    fn pure_helpers_and_env_overrides() {
        assert_eq!(backing_pvc_name("v"), "v-backing");
        assert_eq!(share_pod_name("v"), "share-manager-v");
        assert_eq!(state_cm_name("v"), "state-v");

        let j = backing_pvc_json("pvc-1", "5Gi", "hcloud-volumes");
        assert_eq!(j["spec"]["storageClassName"], "hcloud-volumes");
        assert_eq!(j["spec"]["resources"]["requests"]["storage"], "5Gi");
        assert_eq!(j["spec"]["accessModes"][0], "ReadWriteOnce");
        assert_eq!(j["metadata"]["labels"]["rwx-volume"], "pvc-1");

        // defaults (no env set in this process)
        assert_eq!(share_namespace(), "hcloud-csi-rwx");
        assert_eq!(pull_policy(), "IfNotPresent");
        assert_eq!(backing_storage_class(), "hcloud-volumes");
        assert_eq!(lease_lifetime(), "60");
        assert_eq!(grace_period(), "90");
        assert_eq!(export_mode(), "0777");
        assert_eq!(monitoring_port(), "9587");
        assert!(nfs_allowed_clients().contains("10.0.0.0/8"));
        assert!(image().starts_with("ghcr.io/mrhein/hcloud-csi-rwx:v"));
    }

    #[test]
    fn service_and_pod_specs_are_consistent() {
        let svc = service_json("pvc-x");
        assert_eq!(svc["spec"]["ports"][0]["port"], 2049);
        assert_eq!(svc["spec"]["ports"][1]["port"], 9500);

        let pod = share_pod_json("pvc-x", "n1", "claim", "ns1");
        assert_eq!(pod["spec"]["hostNetwork"], true);
        assert_eq!(pod["spec"]["restartPolicy"], "Never");
        assert_eq!(pod["spec"]["containers"][0]["securityContext"]["privileged"], true);
        assert_eq!(pod["metadata"]["labels"]["rwx-pvc-namespace"], "ns1");
        // recovery token is optional so the pod starts without the secret
        let env = pod["spec"]["containers"][0]["env"].as_array().unwrap();
        let tok = env.iter().find(|e| e["name"] == "RECOVERY_BACKEND_TOKEN").unwrap();
        assert_eq!(tok["valueFrom"]["secretKeyRef"]["optional"], true);
    }
}
