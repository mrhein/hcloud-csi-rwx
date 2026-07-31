//! hcloud-csi-rwx failover controller.
//!
//! Provisioning is handled by the CSI driver (`hcloud-csi-rwx-csi`, driven by
//! csi-provisioner). This controller owns exactly one job: keep the
//! share-manager of every bound RWX volume alive.
//!
//! For each RWX PVC bound to our storage class it watches the share-manager
//! pod (named `share-manager-<volume>`, created by the CSI driver). When the
//! pod fails, vanishes, or its node goes NotReady, the controller:
//!   1. force-deletes VolumeAttachment objects of the backing PVC so
//!      hcloud-csi can detach the block volume from the dead node,
//!   2. evicts workload pods using the RWX claim (breaks stale NFS mounts;
//!      the CSI node plugin re-resolves the NFS endpoint on re-mount),
//!   3. recreates the share-manager pod on a different node
//!      (avoiding `prior_nodes` from the volume's state ConfigMap).
//!
//! NFS endpoint updates need no PV writes: PV sources are immutable, so the
//! node plugin asks the share-manager service for the current endpoint at
//! mount time instead.

use crate::provision;

use std::sync::Arc;

use futures::StreamExt;
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim, Pod, Service};
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::reflector::{Lookup, ObjectRef};
use kube::runtime::watcher;
use kube::{Client, Config};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

const STORAGE_CLASS: &str = provision::DRIVER_NAME;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ShareState {
    volume: String,
    node: Option<String>,
    #[serde(default)]
    prior_nodes: Vec<String>,
    #[serde(default)]
    state: String,
}

impl ShareState {
    fn new(volume: &str) -> Self {
        Self {
            volume: volume.into(),
            state: "starting".into(),
            ..Default::default()
        }
    }
}

struct Ctx {
    client: Client,
}

/// Entry point for the `hcloud-csi-rwx-controller` binary.
pub async fn main() -> anyhow::Result<()> {
    let config = Config::infer().await?;
    let client = Client::try_from(config)?;
    info!(
        storage_class = STORAGE_CLASS,
        namespace = %provision::share_namespace(),
        "failover controller starting"
    );

    let pvc_api: Api<PersistentVolumeClaim> = Api::all(client.clone());
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), &provision::share_namespace());
    let ctx = Arc::new(Ctx { client });

    // Share-manager pod events map back to the RWX PVC via the labels the
    // CSI driver stamps on the pod at creation time.
    let pod_mapper = |pod: Pod| {
        let labels = pod.metadata.labels.unwrap_or_default();
        let name = labels.get("rwx-pvc-name").cloned().unwrap_or_default();
        let ns = labels.get("rwx-pvc-namespace").cloned().unwrap_or_default();
        if name.is_empty() || ns.is_empty() {
            None
        } else {
            Some(ObjectRef::<PersistentVolumeClaim>::new(&name).within(&ns))
        }
    };

    Controller::new(pvc_api, watcher::Config::default())
        .watches(
            pod_api,
            watcher::Config::default().labels("app=hcloud-csi-rwx,rwx-volume"),
            pod_mapper,
        )
        .shutdown_on_signal()
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((oref, _)) => info!(pvc = %oref, "reconciled"),
                Err(e) => error!(error = %e, "reconcile error"),
            }
        })
        .await;

    info!("controller stopped");
    Ok(())
}

fn is_rwx_pvc(pvc: &PersistentVolumeClaim) -> bool {
    let sc = pvc
        .spec
        .as_ref()
        .and_then(|s| s.storage_class_name.as_deref());
    if sc != Some(STORAGE_CLASS) {
        return false;
    }
    pvc.spec
        .as_ref()
        .and_then(|s| s.access_modes.as_ref())
        .map(|m| m.iter().any(|am| am.as_str() == "ReadWriteMany"))
        .unwrap_or(false)
}

/// Reconciler wrapper — logs errors before handing them to the runtime.
async fn reconcile(
    pvc: Arc<PersistentVolumeClaim>,
    ctx: Arc<Ctx>,
) -> Result<Action, kube::Error> {
    let pvc_name = pvc.name().unwrap_or_default().to_string();
    let result = reconcile_body(pvc, ctx).await;
    if let Err(ref e) = result {
        error!(pvc = %pvc_name, error = %e, "reconcile failed");
    }
    result
}

async fn reconcile_body(
    pvc: Arc<PersistentVolumeClaim>,
    ctx: Arc<Ctx>,
) -> Result<Action, kube::Error> {
    let pvc_name = pvc.name().unwrap_or_default().to_string();
    let pvc_ns = pvc.namespace().unwrap_or_default().to_string();

    if !is_rwx_pvc(&pvc) {
        return Ok(Action::requeue(std::time::Duration::from_secs(300)));
    }

    // Until the claim is bound, provisioning is still in the CSI driver's
    // hands — nothing to fail over yet.
    let Some(volume) = pvc
        .spec
        .as_ref()
        .and_then(|s| s.volume_name.clone())
        .filter(|v| !v.is_empty())
    else {
        return Ok(Action::requeue(std::time::Duration::from_secs(30)));
    };

    let vn = provision::volume_name(&volume);
    let ns = provision::share_namespace();
    let pod_api: Api<Pod> = Api::namespaced(ctx.client.clone(), &ns);
    let pod_name = provision::share_pod_name(&vn);

    let mut st = load_or_create_state(&ctx.client, &volume)
        .await
        .map_err(kube_err)?;

    // Self-heal the per-volume service (NFS + endpoint discovery).
    ensure_service(&ctx.client, &volume).await.map_err(kube_err)?;

    match pod_api.get(&pod_name).await {
        Ok(pod) => {
            let phase = pod
                .status
                .as_ref()
                .and_then(|s| s.phase.as_deref())
                .unwrap_or("Unknown")
                .to_string();
            let pod_node = pod.spec.as_ref().and_then(|s| s.node_name.clone());

            match phase.as_str() {
                "Running" => {
                    // A dead kubelet leaves the pod object "Running" — check
                    // the node itself.
                    if let Some(node) = &pod_node
                        && !provision::node_is_ready(&ctx.client, node).await {
                            warn!(pvc = %pvc_name, node = %node, "share-manager node NotReady, failing over");
                            return start_failover(&ctx, &mut st, &volume, &pvc_name, &pvc_ns, &pod_api, &pod_name).await;
                        }
                    if st.state != "running" || st.node != pod_node {
                        st.node = pod_node;
                        st.state = "running".into();
                        save_state(&ctx.client, &st).await.map_err(kube_err)?;
                        info!(pvc = %pvc_name, node = ?st.node, "share-manager running");
                    }
                    Ok(Action::requeue(std::time::Duration::from_secs(60)))
                }
                // "Succeeded" too: with restartPolicy Never an exited
                // share-manager (e.g. ganesha died) stays dead otherwise.
                "Failed" | "Unknown" | "Succeeded" => {
                    warn!(pvc = %pvc_name, phase = %phase, "share-manager unhealthy, failing over");
                    start_failover(&ctx, &mut st, &volume, &pvc_name, &pvc_ns, &pod_api, &pod_name).await
                }
                _ => Ok(Action::requeue(std::time::Duration::from_secs(10))),
            }
        }
        Err(kube::Error::Api(e)) if e.code == 404 => {
            // Share-manager pod is gone (failover in progress, node drained,
            // or deleted externally). Recreate it on a fresh node.
            if st.state != "failing_over" {
                st.state = "failing_over".into();
                save_state(&ctx.client, &st).await.map_err(kube_err)?;
                // Workloads still hold mounts to the old endpoint.
                evict_workload_pods(&ctx.client, &pvc_name, &pvc_ns)
                    .await
                    .map_err(kube_err)?;
            }

            if let Err(e) = force_detach_backing_pvc(&ctx.client, &vn).await {
                warn!(pvc = %pvc_name, error = %e, "force-detach failed, will retry");
                return Ok(Action::requeue(std::time::Duration::from_secs(5)));
            }
            if is_backing_pvc_attached(&ctx.client, &vn).await {
                info!(pvc = %pvc_name, "waiting for backing volume to detach");
                return Ok(Action::requeue(std::time::Duration::from_secs(3)));
            }

            if let Some(n) = st.node.take()
                && !st.prior_nodes.contains(&n) {
                    st.prior_nodes.push(n);
                }
            let node = pick_node_with_reset(&ctx.client, &mut st).await.map_err(kube_err)?;
            info!(pvc = %pvc_name, node = %node, prior = ?st.prior_nodes, "recreating share-manager pod");
            st.node = Some(node.clone());
            st.state = "starting".into();
            save_state(&ctx.client, &st).await.map_err(kube_err)?;

            let pod: Pod =
                serde_json::from_value(provision::share_pod_json(&volume, &node, &pvc_name, &pvc_ns))
                    .map_err(|e| kube_err(anyhow::anyhow!(e)))?;
            match pod_api.create(&Default::default(), &pod).await {
                Ok(_) => {}
                Err(kube::Error::Api(e)) if e.code == 409 => {} // raced with CSI driver
                Err(e) => return Err(e),
            }
            Ok(Action::requeue(std::time::Duration::from_secs(5)))
        }
        Err(e) => Err(e),
    }
}

async fn start_failover(
    ctx: &Ctx,
    st: &mut ShareState,
    volume: &str,
    pvc_name: &str,
    pvc_ns: &str,
    pod_api: &Api<Pod>,
    pod_name: &str,
) -> Result<Action, kube::Error> {
    let vn = provision::volume_name(volume);
    st.state = "failing_over".into();
    if let Some(n) = st.node.take()
        && !st.prior_nodes.contains(&n) {
            st.prior_nodes.push(n);
        }
    save_state(&ctx.client, st).await.map_err(kube_err)?;

    force_detach_backing_pvc(&ctx.client, &vn)
        .await
        .map_err(kube_err)?;
    evict_workload_pods(&ctx.client, pvc_name, pvc_ns)
        .await
        .map_err(kube_err)?;
    let _ = pod_api.delete(pod_name, &DeleteParams::default()).await;
    Ok(Action::requeue(std::time::Duration::from_secs(3)))
}

fn error_policy(
    _obj: Arc<PersistentVolumeClaim>,
    _err: &kube::Error,
    _ctx: Arc<Ctx>,
) -> Action {
    Action::requeue(std::time::Duration::from_secs(30))
}

/// Convert anyhow errors to kube::Error.
fn kube_err(e: anyhow::Error) -> kube::Error {
    kube::Error::Api(kube::core::ErrorResponse {
        status: "InternalError".into(),
        message: e.to_string(),
        reason: "ReconcileError".into(),
        code: 500,
    })
}

// ── State persistence ──

async fn load_or_create_state(client: &Client, volume: &str) -> anyhow::Result<ShareState> {
    let ns = provision::share_namespace();
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), &ns);
    let cm_name = provision::state_cm_name(&provision::volume_name(volume));
    match cm_api.get(&cm_name).await {
        Ok(cm) => {
            let data = cm
                .data
                .as_ref()
                .and_then(|d| d.get("state.json"))
                .map(|s| s.as_str())
                .unwrap_or("{}");
            let st: ShareState =
                serde_json::from_str(data).unwrap_or_else(|_| ShareState::new(volume));
            Ok(st)
        }
        Err(kube::Error::Api(e)) if e.code == 404 => {
            let st = ShareState::new(volume);
            let cm = state_cm(&cm_name, &st);
            match cm_api.create(&Default::default(), &cm).await {
                Ok(_) => {}
                Err(kube::Error::Api(e)) if e.code == 409 => {}
                Err(e) => return Err(e.into()),
            }
            Ok(st)
        }
        Err(e) => Err(e.into()),
    }
}

fn state_cm(name: &str, st: &ShareState) -> ConfigMap {
    ConfigMap {
        metadata: kube::api::ObjectMeta {
            name: Some(name.into()),
            namespace: Some(provision::share_namespace()),
            ..Default::default()
        },
        data: Some(
            [(
                "state.json".to_string(),
                serde_json::to_string(st).unwrap_or_default(),
            )]
            .into_iter()
            .collect(),
        ),
        ..Default::default()
    }
}

async fn save_state(client: &Client, st: &ShareState) -> anyhow::Result<()> {
    let ns = provision::share_namespace();
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), &ns);
    let cm_name = provision::state_cm_name(&provision::volume_name(&st.volume));
    let cm = state_cm(&cm_name, st);
    cm_api
        .patch(
            &cm_name,
            &PatchParams::apply("hcloud-csi-rwx-controller"),
            &Patch::Merge(&cm),
        )
        .await?;
    Ok(())
}

// ── Node selection ──

/// Pick a node avoiding `prior_nodes`; when every node has been used already,
/// forget all but the most recent failure so the volume stays schedulable.
async fn pick_node_with_reset(client: &Client, st: &mut ShareState) -> anyhow::Result<String> {
    match provision::pick_node(client, &st.prior_nodes).await {
        Ok(n) => Ok(n),
        Err(_) if st.prior_nodes.len() > 1 => {
            let last = st.prior_nodes.last().cloned();
            st.prior_nodes = last.into_iter().collect();
            info!(volume = %st.volume, "all nodes tried, resetting prior_nodes");
            provision::pick_node(client, &st.prior_nodes).await
        }
        Err(e) => Err(e),
    }
}

// ── Service ──

async fn ensure_service(client: &Client, volume: &str) -> anyhow::Result<()> {
    let ns = provision::share_namespace();
    let svc_api: Api<Service> = Api::namespaced(client.clone(), &ns);
    let vn = provision::volume_name(volume);
    if svc_api.get(&vn).await.is_ok() {
        return Ok(());
    }
    let svc: Service = serde_json::from_value(provision::service_json(volume))?;
    match svc_api.create(&Default::default(), &svc).await {
        Ok(_) => info!(svc = %vn, "created Service"),
        Err(kube::Error::Api(e)) if e.code == 409 => {}
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

// ── Multi-node failover: force-detach backing PVC ──

/// Force-detach the backing PVC by deleting VolumeAttachment objects that
/// reference its PV. This lets hcloud-csi detach the block volume from the
/// dead node so it can re-attach elsewhere.
async fn force_detach_backing_pvc(client: &Client, vn: &str) -> anyhow::Result<()> {
    let Some(backing_pv) = backing_pv_name(client, vn).await else {
        return Ok(());
    };

    use k8s_openapi::api::storage::v1::VolumeAttachment;
    let va_api: Api<VolumeAttachment> = Api::all(client.clone());
    for va in va_api.list(&ListParams::default()).await? {
        let va_name = va.name().unwrap_or_default().to_string();
        let pv_name = va
            .spec
            .source
            .persistent_volume_name
            .as_deref()
            .unwrap_or("");
        if pv_name == backing_pv {
            info!(va = %va_name, pv = %pv_name, "force-detaching backing volume");
            let _ = va_api.delete(&va_name, &DeleteParams::default()).await;
        }
    }
    Ok(())
}

/// Check if the backing PVC still has an active VolumeAttachment.
async fn is_backing_pvc_attached(client: &Client, vn: &str) -> bool {
    let Some(backing_pv) = backing_pv_name(client, vn).await else {
        return false;
    };

    use k8s_openapi::api::storage::v1::VolumeAttachment;
    let va_api: Api<VolumeAttachment> = Api::all(client.clone());
    match va_api.list(&ListParams::default()).await {
        Ok(vas) => vas.into_iter().any(|va| {
            va.spec
                .source
                .persistent_volume_name
                .as_deref()
                .unwrap_or("")
                == backing_pv
        }),
        Err(_) => false,
    }
}

/// PV name bound to the backing PVC, if any.
async fn backing_pv_name(client: &Client, vn: &str) -> Option<String> {
    let ns = provision::share_namespace();
    let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), &ns);
    pvc_api
        .get(&provision::backing_pvc_name(vn))
        .await
        .ok()?
        .spec
        .as_ref()?
        .volume_name
        .clone()
        .filter(|v| !v.is_empty())
}

// ── Failover: evict workload pods ──

/// Delete all pods in the RWX claim's namespace that mount it, so they get
/// rescheduled and re-mount via the fresh NFS endpoint.
async fn evict_workload_pods(client: &Client, pvc_name: &str, pvc_ns: &str) -> anyhow::Result<()> {
    if pvc_name.is_empty() || pvc_ns.is_empty() {
        return Ok(());
    }
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), pvc_ns);
    for pod in pod_api.list(&ListParams::default()).await? {
        let has_claim = pod
            .spec
            .as_ref()
            .and_then(|s| s.volumes.as_ref())
            .map(|v| {
                v.iter().any(|vol| {
                    vol.persistent_volume_claim
                        .as_ref()
                        .is_some_and(|pvc| pvc.claim_name == pvc_name)
                })
            })
            .unwrap_or(false);
        if has_claim {
            let pod_name = pod.name().unwrap_or_default().to_string();
            warn!(pod = %pod_name, ns = %pvc_ns, "evicting workload pod for NFS remount");
            let _ = pod_api.delete(&pod_name, &DeleteParams::default()).await;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rwx_pvc(sc: &str, modes: &[&str]) -> PersistentVolumeClaim {
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": { "name": "test", "namespace": "default" },
            "spec": {
                "storageClassName": sc,
                "accessModes": modes,
                "resources": { "requests": { "storage": "1Gi" } }
            }
        }))
        .unwrap()
    }

    #[test]
    fn rwx_pvc_detection() {
        assert!(is_rwx_pvc(&rwx_pvc("hcloud-csi-rwx", &["ReadWriteMany"])));
        assert!(!is_rwx_pvc(&rwx_pvc("hcloud-csi-rwx", &["ReadWriteOnce"])));
        assert!(!is_rwx_pvc(&rwx_pvc("hcloud-volumes", &["ReadWriteMany"])));
    }

    #[test]
    fn state_roundtrip_tolerates_old_format() {
        // state.json written by v0.1.0 had more fields — must still parse.
        let old = r#"{"volume":"vol","backing_pvc":"vol-backing","share_pod":"share-manager-vol","service":"vol","pv":"rwx-vol","node":"n1","prior_nodes":["n0"],"endpoint":"1.2.3.4:/vol","state":"running"}"#;
        let st: ShareState = serde_json::from_str(old).unwrap();
        assert_eq!(st.volume, "vol");
        assert_eq!(st.node.as_deref(), Some("n1"));
        assert_eq!(st.prior_nodes, vec!["n0".to_string()]);
    }
}

#[cfg(test)]
mod reconcile_tests {
    use super::*;
    use crate::testing::{
        empty_list, node_list, pod, pod_list, pvc, service, state_cm as cm_fixture,
        va_list, volume_attachment, FakeApi,
    };
    use axum::http::StatusCode;
    use serde_json::json;
    use std::sync::Arc;

    const NS: &str = "/api/v1/namespaces/hcloud-csi-rwx";
    const VOL: &str = "pvc-1";
    const SM: &str = "share-manager-pvc-1";

    fn ctx(fake: &FakeApi) -> Arc<Ctx> {
        Arc::new(Ctx { client: fake.client() })
    }

    fn rwx_claim(volume: Option<&str>) -> Arc<PersistentVolumeClaim> {
        Arc::new(
            serde_json::from_value(pvc(
                "rwx-test",
                "default",
                "hcloud-csi-rwx",
                &["ReadWriteMany"],
                volume,
            ))
            .unwrap(),
        )
    }

    /// Baseline routes so a reconcile can get past state + service handling.
    fn base(fake: FakeApi, state: &str) -> FakeApi {
        fake.ok(&format!("GET {NS}/configmaps/state-pvc-1"), cm_fixture("state-pvc-1", state))
            .ok(&format!("PATCH {NS}/configmaps/state-pvc-1"), cm_fixture("state-pvc-1", state))
            .ok(&format!("GET {NS}/services/pvc-1"), service("pvc-1", "10.43.0.9"))
    }

    // ── PVC filtering ──

    #[test]
    fn only_rwx_claims_of_our_storage_class_qualify() {
        let mk = |sc: &str, modes: &[&str]| -> PersistentVolumeClaim {
            serde_json::from_value(pvc("c", "default", sc, modes, None)).unwrap()
        };
        assert!(is_rwx_pvc(&mk("hcloud-csi-rwx", &["ReadWriteMany"])));
        assert!(!is_rwx_pvc(&mk("hcloud-csi-rwx", &["ReadWriteOnce"])));
        assert!(!is_rwx_pvc(&mk("hcloud-volumes", &["ReadWriteMany"])));

        let bare: PersistentVolumeClaim = serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": "c", "namespace": "default"}
        }))
        .unwrap();
        assert!(!is_rwx_pvc(&bare));
    }

    #[tokio::test]
    async fn foreign_claims_are_requeued_without_api_calls() {
        let fake = FakeApi::new();
        let claim: Arc<PersistentVolumeClaim> = Arc::new(
            serde_json::from_value(pvc("c", "default", "hcloud-volumes", &["ReadWriteOnce"], None))
                .unwrap(),
        );
        let action = reconcile(claim, ctx(&fake)).await.unwrap();
        assert_eq!(action, Action::requeue(std::time::Duration::from_secs(300)));
        assert!(fake.seen().is_empty(), "must not touch the API");
    }

    #[tokio::test]
    async fn unbound_claims_wait_for_the_provisioner() {
        let fake = FakeApi::new();
        let action = reconcile(rwx_claim(None), ctx(&fake)).await.unwrap();
        assert_eq!(action, Action::requeue(std::time::Duration::from_secs(30)));
        assert!(fake.seen().is_empty());
    }

    // ── Healthy path ──

    #[tokio::test]
    async fn running_share_manager_is_recorded_and_requeued() {
        let fake = base(FakeApi::new(), r#"{"volume":"pvc-1","state":"starting"}"#)
            .ok(&format!("GET {NS}/pods/{SM}"), pod(SM, "n1", "Running"))
            .ok("GET /api/v1/nodes/n1", crate::testing::node("n1", true));

        let action = reconcile(rwx_claim(Some(VOL)), ctx(&fake)).await.unwrap();
        assert_eq!(action, Action::requeue(std::time::Duration::from_secs(60)));

        let saved = fake.bodies(&format!("PATCH {NS}/configmaps/state-pvc-1"));
        assert!(saved.iter().any(|b| b.contains("running")), "state not persisted: {saved:?}");
        assert!(saved.iter().any(|b| b.contains("n1")));
    }

    #[tokio::test]
    async fn pods_still_starting_are_polled_again() {
        let fake = base(FakeApi::new(), r#"{"volume":"pvc-1","state":"starting"}"#)
            .ok(&format!("GET {NS}/pods/{SM}"), pod(SM, "n1", "Pending"));
        let action = reconcile(rwx_claim(Some(VOL)), ctx(&fake)).await.unwrap();
        assert_eq!(action, Action::requeue(std::time::Duration::from_secs(10)));
    }

    // ── Failover triggers ──

    #[tokio::test]
    async fn failed_pod_triggers_failover() {
        let fake = base(FakeApi::new(), r#"{"volume":"pvc-1","node":"n1","state":"running"}"#)
            .ok(&format!("GET {NS}/pods/{SM}"), pod(SM, "n1", "Failed"))
            .ok(&format!("GET {NS}/persistentvolumeclaims/pvc-1-backing"),
                pvc("pvc-1-backing", "hcloud-csi-rwx", "hcloud-volumes", &["ReadWriteOnce"], Some("pv-1")))
            .ok("GET /apis/storage.k8s.io/v1/volumeattachments", va_list(vec![volume_attachment("va1", "pv-1")]))
            .ok("DELETE /apis/storage.k8s.io/v1/volumeattachments/va1", json!({}))
            .ok("GET /api/v1/namespaces/default/pods", empty_list("PodList"))
            .ok(&format!("DELETE {NS}/pods/{SM}"), json!({}));

        let action = reconcile(rwx_claim(Some(VOL)), ctx(&fake)).await.unwrap();
        assert_eq!(action, Action::requeue(std::time::Duration::from_secs(3)));
        assert!(fake.called("DELETE /apis/storage.k8s.io/v1/volumeattachments/va1"),
                "backing volume must be force-detached");
        assert!(fake.called(&format!("DELETE {NS}/pods/{SM}")));
        let saved = fake.bodies(&format!("PATCH {NS}/configmaps/state-pvc-1"));
        assert!(saved.iter().any(|b| b.contains("failing_over")));
        assert!(saved.iter().any(|b| b.contains("prior_nodes")), "node must be remembered");
    }

    #[tokio::test]
    async fn succeeded_pod_also_triggers_failover() {
        // restartPolicy: Never — a share-manager whose ganesha exited cleanly
        // would otherwise stay dead forever.
        let fake = base(FakeApi::new(), r#"{"volume":"pvc-1","node":"n1","state":"running"}"#)
            .ok(&format!("GET {NS}/pods/{SM}"), pod(SM, "n1", "Succeeded"))
            .ok(&format!("GET {NS}/persistentvolumeclaims/pvc-1-backing"),
                pvc("pvc-1-backing", "hcloud-csi-rwx", "hcloud-volumes", &["ReadWriteOnce"], Some("pv-1")))
            .ok("GET /apis/storage.k8s.io/v1/volumeattachments", va_list(vec![]))
            .ok("GET /api/v1/namespaces/default/pods", empty_list("PodList"))
            .ok(&format!("DELETE {NS}/pods/{SM}"), json!({}));

        reconcile(rwx_claim(Some(VOL)), ctx(&fake)).await.unwrap();
        assert!(fake.called(&format!("DELETE {NS}/pods/{SM}")));
    }

    #[tokio::test]
    async fn running_pod_on_a_notready_node_triggers_failover() {
        let fake = base(FakeApi::new(), r#"{"volume":"pvc-1","node":"n1","state":"running"}"#)
            .ok(&format!("GET {NS}/pods/{SM}"), pod(SM, "n1", "Running"))
            .ok("GET /api/v1/nodes/n1", crate::testing::node("n1", false))
            .ok(&format!("GET {NS}/persistentvolumeclaims/pvc-1-backing"),
                pvc("pvc-1-backing", "hcloud-csi-rwx", "hcloud-volumes", &["ReadWriteOnce"], Some("pv-1")))
            .ok("GET /apis/storage.k8s.io/v1/volumeattachments", va_list(vec![]))
            .ok("GET /api/v1/namespaces/default/pods", empty_list("PodList"))
            .ok(&format!("DELETE {NS}/pods/{SM}"), json!({}));

        reconcile(rwx_claim(Some(VOL)), ctx(&fake)).await.unwrap();
        assert!(fake.called(&format!("DELETE {NS}/pods/{SM}")));
    }

    #[tokio::test]
    async fn workload_pods_of_the_claim_are_evicted() {
        let workload = json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "app-1", "namespace": "default"},
            "spec": {"volumes": [
                {"name": "d", "persistentVolumeClaim": {"claimName": "rwx-test"}}
            ]},
            "status": {"phase": "Running"}
        });
        let unrelated = json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "other", "namespace": "default"},
            "spec": {"volumes": [
                {"name": "d", "persistentVolumeClaim": {"claimName": "something-else"}}
            ]},
            "status": {"phase": "Running"}
        });
        let fake = FakeApi::new()
            .ok("GET /api/v1/namespaces/default/pods", pod_list(vec![workload, unrelated]))
            .ok("DELETE /api/v1/namespaces/default/pods/app-1", json!({}));

        evict_workload_pods(&fake.client(), "rwx-test", "default").await.unwrap();
        assert!(fake.called("DELETE /api/v1/namespaces/default/pods/app-1"));
        assert!(!fake.called("DELETE /api/v1/namespaces/default/pods/other"),
                "must not evict unrelated workloads");
    }

    #[tokio::test]
    async fn eviction_is_skipped_without_claim_identity() {
        let fake = FakeApi::new();
        evict_workload_pods(&fake.client(), "", "default").await.unwrap();
        evict_workload_pods(&fake.client(), "claim", "").await.unwrap();
        assert!(fake.seen().is_empty());
    }

    // ── Recreating the share-manager ──

    #[tokio::test]
    async fn missing_pod_is_recreated_on_a_different_node() {
        let fake = base(FakeApi::new(), r#"{"volume":"pvc-1","node":"n1","state":"failing_over"}"#)
            .err(&format!("GET {NS}/pods/{SM}"), StatusCode::NOT_FOUND)
            .ok(&format!("GET {NS}/persistentvolumeclaims/pvc-1-backing"),
                pvc("pvc-1-backing", "hcloud-csi-rwx", "hcloud-volumes", &["ReadWriteOnce"], Some("pv-1")))
            .ok("GET /apis/storage.k8s.io/v1/volumeattachments", va_list(vec![]))
            .ok(&format!("GET {NS}/pods?"), empty_list("PodList"))
            .ok("GET /api/v1/nodes", node_list(&[("n1", true), ("n2", true)]))
            .ok(&format!("POST {NS}/pods"), json!({}));

        let action = reconcile(rwx_claim(Some(VOL)), ctx(&fake)).await.unwrap();
        assert_eq!(action, Action::requeue(std::time::Duration::from_secs(5)));

        let created = fake.bodies(&format!("POST {NS}/pods"));
        assert!(created[0].contains("\"kubernetes.io/hostname\":\"n2\""),
                "must avoid the previous node: {}", created[0]);
        assert!(created[0].contains("rwx-pvc-name"), "claim identity must be stamped on the pod");
    }

    #[tokio::test]
    async fn recreation_waits_while_the_volume_is_still_attached() {
        let fake = base(FakeApi::new(), r#"{"volume":"pvc-1","node":"n1","state":"failing_over"}"#)
            .err(&format!("GET {NS}/pods/{SM}"), StatusCode::NOT_FOUND)
            .ok(&format!("GET {NS}/persistentvolumeclaims/pvc-1-backing"),
                pvc("pvc-1-backing", "hcloud-csi-rwx", "hcloud-volumes", &["ReadWriteOnce"], Some("pv-1")))
            .ok("GET /apis/storage.k8s.io/v1/volumeattachments", va_list(vec![volume_attachment("va1", "pv-1")]))
            .ok("DELETE /apis/storage.k8s.io/v1/volumeattachments/va1", json!({}));

        let action = reconcile(rwx_claim(Some(VOL)), ctx(&fake)).await.unwrap();
        assert_eq!(action, Action::requeue(std::time::Duration::from_secs(3)));
        assert!(!fake.called(&format!("POST {NS}/pods")), "must not start before detach");
    }

    #[tokio::test]
    async fn recreation_tolerates_a_race_with_the_csi_driver() {
        let fake = base(FakeApi::new(), r#"{"volume":"pvc-1","state":"failing_over"}"#)
            .err(&format!("GET {NS}/pods/{SM}"), StatusCode::NOT_FOUND)
            .err(&format!("GET {NS}/persistentvolumeclaims/pvc-1-backing"), StatusCode::NOT_FOUND)
            .ok("GET /apis/storage.k8s.io/v1/volumeattachments", va_list(vec![]))
            .ok(&format!("GET {NS}/pods?"), empty_list("PodList"))
            .ok("GET /api/v1/nodes", node_list(&[("n1", true)]))
            .err(&format!("POST {NS}/pods"), StatusCode::CONFLICT);

        // 409 means the CSI driver created it first — not an error.
        assert!(reconcile(rwx_claim(Some(VOL)), ctx(&fake)).await.is_ok());
    }

    // ── Node selection with exhausted history ──

    #[tokio::test]
    async fn prior_nodes_reset_when_every_node_was_tried() {
        let fake = FakeApi::new()
            .ok(&format!("GET {NS}/pods?"), empty_list("PodList"))
            .ok("GET /api/v1/nodes", node_list(&[("n1", true), ("n2", true)]));
        let mut st = ShareState {
            volume: VOL.into(),
            prior_nodes: vec!["n1".into(), "n2".into()],
            ..Default::default()
        };
        let node = pick_node_with_reset(&fake.client(), &mut st).await.unwrap();
        assert_eq!(node, "n1", "oldest failure is forgotten first");
        assert_eq!(st.prior_nodes, vec!["n2".to_string()], "only the latest failure is kept");
    }

    #[tokio::test]
    async fn single_prior_node_is_not_reset() {
        let fake = FakeApi::new()
            .ok(&format!("GET {NS}/pods?"), empty_list("PodList"))
            .ok("GET /api/v1/nodes", node_list(&[("n1", true)]));
        let mut st = ShareState {
            volume: VOL.into(),
            prior_nodes: vec!["n1".into()],
            ..Default::default()
        };
        assert!(pick_node_with_reset(&fake.client(), &mut st).await.is_err());
    }

    // ── State persistence ──

    #[tokio::test]
    async fn state_is_created_when_absent_and_survives_a_race() {
        let created = FakeApi::new()
            .err(&format!("GET {NS}/configmaps/state-pvc-1"), StatusCode::NOT_FOUND)
            .ok(&format!("POST {NS}/configmaps"), cm_fixture("state-pvc-1", "{}"));
        let st = load_or_create_state(&created.client(), VOL).await.unwrap();
        assert_eq!(st.volume, VOL);
        assert_eq!(st.state, "starting");

        let raced = FakeApi::new()
            .err(&format!("GET {NS}/configmaps/state-pvc-1"), StatusCode::NOT_FOUND)
            .err(&format!("POST {NS}/configmaps"), StatusCode::CONFLICT);
        assert!(load_or_create_state(&raced.client(), VOL).await.is_ok());
    }

    #[tokio::test]
    async fn corrupt_state_falls_back_to_a_fresh_one() {
        let fake = FakeApi::new()
            .ok(&format!("GET {NS}/configmaps/state-pvc-1"), cm_fixture("state-pvc-1", "not json"));
        let st = load_or_create_state(&fake.client(), VOL).await.unwrap();
        assert_eq!(st.state, "starting");
    }

    #[tokio::test]
    async fn state_load_propagates_unexpected_errors() {
        let fake = FakeApi::new()
            .err(&format!("GET {NS}/configmaps/state-pvc-1"), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(load_or_create_state(&fake.client(), VOL).await.is_err());
    }

    #[tokio::test]
    async fn old_state_format_still_parses() {
        let legacy = r#"{"volume":"vol","backing_pvc":"vol-backing","share_pod":"share-manager-vol",
            "service":"vol","pv":"rwx-vol","node":"n1","prior_nodes":["n0"],
            "endpoint":"1.2.3.4:/vol","state":"running"}"#;
        let st: ShareState = serde_json::from_str(legacy).unwrap();
        assert_eq!(st.node.as_deref(), Some("n1"));
        assert_eq!(st.prior_nodes, vec!["n0".to_string()]);
    }

    #[tokio::test]
    async fn save_state_writes_the_configmap() {
        let fake = FakeApi::new()
            .ok(&format!("PATCH {NS}/configmaps/state-pvc-1"), cm_fixture("state-pvc-1", "{}"));
        let st = ShareState { volume: VOL.into(), state: "running".into(), ..Default::default() };
        save_state(&fake.client(), &st).await.unwrap();
        assert!(fake.bodies(&format!("PATCH {NS}/configmaps"))[0].contains("running"));
    }

    #[test]
    fn state_configmap_is_namespaced_and_named() {
        let st = ShareState { volume: VOL.into(), ..Default::default() };
        let cm = state_cm("state-pvc-1", &st);
        assert_eq!(cm.metadata.name.as_deref(), Some("state-pvc-1"));
        assert_eq!(cm.metadata.namespace.as_deref(), Some("hcloud-csi-rwx"));
        assert!(cm.data.unwrap()["state.json"].contains("pvc-1"));
    }

    // ── Service self-healing ──

    #[tokio::test]
    async fn service_is_created_when_missing_and_reused_otherwise() {
        let missing = FakeApi::new()
            .err(&format!("GET {NS}/services/pvc-1"), StatusCode::NOT_FOUND)
            .ok(&format!("POST {NS}/services"), json!({}));
        ensure_service(&missing.client(), VOL).await.unwrap();
        assert!(missing.called(&format!("POST {NS}/services")));

        let present = FakeApi::new().ok(&format!("GET {NS}/services/pvc-1"), service("pvc-1", "10.43.0.9"));
        ensure_service(&present.client(), VOL).await.unwrap();
        assert!(!present.called(&format!("POST {NS}/services")));

        let raced = FakeApi::new()
            .err(&format!("GET {NS}/services/pvc-1"), StatusCode::NOT_FOUND)
            .err(&format!("POST {NS}/services"), StatusCode::CONFLICT);
        assert!(ensure_service(&raced.client(), VOL).await.is_ok());

        let broken = FakeApi::new()
            .err(&format!("GET {NS}/services/pvc-1"), StatusCode::NOT_FOUND)
            .err(&format!("POST {NS}/services"), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(ensure_service(&broken.client(), VOL).await.is_err());
    }

    // ── Backing volume plumbing ──

    #[tokio::test]
    async fn backing_pv_name_resolution() {
        let bound = FakeApi::new().ok(
            &format!("GET {NS}/persistentvolumeclaims/pvc-1-backing"),
            pvc("pvc-1-backing", "hcloud-csi-rwx", "hcloud-volumes", &["ReadWriteOnce"], Some("pv-1")),
        );
        assert_eq!(backing_pv_name(&bound.client(), VOL).await.as_deref(), Some("pv-1"));

        let unbound = FakeApi::new().ok(
            &format!("GET {NS}/persistentvolumeclaims/pvc-1-backing"),
            pvc("pvc-1-backing", "hcloud-csi-rwx", "hcloud-volumes", &["ReadWriteOnce"], None),
        );
        assert!(backing_pv_name(&unbound.client(), VOL).await.is_none());

        let gone = FakeApi::new();
        assert!(backing_pv_name(&gone.client(), VOL).await.is_none());
    }

    #[tokio::test]
    async fn force_detach_only_touches_our_attachment() {
        let fake = FakeApi::new()
            .ok(&format!("GET {NS}/persistentvolumeclaims/pvc-1-backing"),
                pvc("pvc-1-backing", "hcloud-csi-rwx", "hcloud-volumes", &["ReadWriteOnce"], Some("pv-1")))
            .ok("GET /apis/storage.k8s.io/v1/volumeattachments",
                va_list(vec![volume_attachment("va1", "pv-1"), volume_attachment("va2", "pv-other")]))
            .ok("DELETE /apis/storage.k8s.io/v1/volumeattachments/va1", json!({}));

        force_detach_backing_pvc(&fake.client(), VOL).await.unwrap();
        assert!(fake.called("DELETE /apis/storage.k8s.io/v1/volumeattachments/va1"));
        assert!(!fake.called("DELETE /apis/storage.k8s.io/v1/volumeattachments/va2"),
                "must not detach someone else's volume");
    }

    #[tokio::test]
    async fn force_detach_is_a_noop_without_a_backing_pv() {
        let fake = FakeApi::new();
        force_detach_backing_pvc(&fake.client(), VOL).await.unwrap();
        assert!(!fake.called("volumeattachments"));
    }

    #[tokio::test]
    async fn attachment_probe_reflects_reality() {
        let attached = FakeApi::new()
            .ok(&format!("GET {NS}/persistentvolumeclaims/pvc-1-backing"),
                pvc("pvc-1-backing", "hcloud-csi-rwx", "hcloud-volumes", &["ReadWriteOnce"], Some("pv-1")))
            .ok("GET /apis/storage.k8s.io/v1/volumeattachments", va_list(vec![volume_attachment("va1", "pv-1")]));
        assert!(is_backing_pvc_attached(&attached.client(), VOL).await);

        let detached = FakeApi::new()
            .ok(&format!("GET {NS}/persistentvolumeclaims/pvc-1-backing"),
                pvc("pvc-1-backing", "hcloud-csi-rwx", "hcloud-volumes", &["ReadWriteOnce"], Some("pv-1")))
            .ok("GET /apis/storage.k8s.io/v1/volumeattachments", va_list(vec![]));
        assert!(!is_backing_pvc_attached(&detached.client(), VOL).await);

        let no_pv = FakeApi::new();
        assert!(!is_backing_pvc_attached(&no_pv.client(), VOL).await);

        let api_down = FakeApi::new()
            .ok(&format!("GET {NS}/persistentvolumeclaims/pvc-1-backing"),
                pvc("pvc-1-backing", "hcloud-csi-rwx", "hcloud-volumes", &["ReadWriteOnce"], Some("pv-1")))
            .err("GET /apis/storage.k8s.io/v1/volumeattachments", StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!is_backing_pvc_attached(&api_down.client(), VOL).await);
    }

    // ── Error plumbing ──

    #[tokio::test]
    async fn helpers_wrap_errors_and_requeue() {
        let e = kube_err(anyhow::anyhow!("boom"));
        match e {
            kube::Error::Api(r) => {
                assert_eq!(r.code, 500);
                assert!(r.message.contains("boom"));
            }
            other => panic!("unexpected {other:?}"),
        }

        let action = error_policy(
            rwx_claim(None),
            &kube::Error::LinesCodecMaxLineLengthExceeded,
            Arc::new(Ctx { client: FakeApi::new().client() }),
        );
        assert_eq!(action, Action::requeue(std::time::Duration::from_secs(30)));
    }

    #[tokio::test]
    async fn reconcile_surfaces_api_errors() {
        let fake = FakeApi::new()
            .err(&format!("GET {NS}/configmaps/state-pvc-1"), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(reconcile(rwx_claim(Some(VOL)), ctx(&fake)).await.is_err());
    }
}
