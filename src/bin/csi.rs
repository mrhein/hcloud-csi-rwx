//! hcloud-csi-rwx CSI driver gRPC server.
//!
//! Implements the Container Storage Interface spec for RWX volumes.
//! - Identity: GetPluginInfo, GetPluginCapabilities, Probe
//! - Controller: CreateVolume (backing PVC + share-manager + service), DeleteVolume
//! - Node: NodePublishVolume (mount NFS), NodeUnpublishVolume (umount)
//!
//! Failover after provisioning is handled by `hcloud-csi-rwx-controller`.
//! Because PV sources are immutable, NodePublishVolume asks the share-manager
//! service for the *current* NFS endpoint before mounting and only falls back
//! to the endpoint captured in `volume_context` at provisioning time.

// Shared with bin/controller.rs — each binary compiles its own copy, so
// helpers used only by the other binary would otherwise trip dead_code.
#[allow(dead_code)]
#[path = "../provision.rs"]
mod provision;

use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim, Pod, Service};
use kube::api::{Api, DeleteParams};
use kube::{Client as KubeClient, Config};

use tokio_stream::wrappers::UnixListenerStream;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, warn};

mod csi {
    // Generated from the upstream CSI spec — its doc comments and enum
    // naming trip clippy.
    #![allow(clippy::doc_overindented_list_items)]
    #![allow(clippy::doc_lazy_continuation)]
    #![allow(clippy::enum_variant_names)]
    tonic::include_proto!("csi.v1");
}

use csi::*;
use provision::{volume_name, DRIVER_NAME};

const GIB: i64 = 1_073_741_824;

#[derive(Clone)]
struct CsiDriver {
    kube: KubeClient,
}

fn st(err: impl std::fmt::Display) -> Status {
    Status::internal(err.to_string())
}

/// Round the requested bytes up to whole GiB (at least 1GiB, default 10GiB).
fn requested_gib(capacity: Option<&CapacityRange>) -> i64 {
    capacity
        .map(|c| c.required_bytes)
        .filter(|b| *b > 0)
        .map(|b| (b + GIB - 1) / GIB)
        .unwrap_or(10)
}

// ── Identity Service ──

#[tonic::async_trait]
impl identity_server::Identity for CsiDriver {
    async fn get_plugin_info(
        &self,
        _req: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        Ok(Response::new(GetPluginInfoResponse {
            name: DRIVER_NAME.into(),
            vendor_version: env!("CARGO_PKG_VERSION").into(),
            manifest: Default::default(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _req: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        Ok(Response::new(GetPluginCapabilitiesResponse {
            capabilities: vec![
                PluginCapability {
                    r#type: Some(plugin_capability::Type::Service(plugin_capability::Service {
                        r#type: plugin_capability::service::Type::ControllerService as i32,
                    })),
                },
            ],
        }))
    }

    async fn probe(
        &self,
        _req: Request<ProbeRequest>,
    ) -> Result<Response<ProbeResponse>, Status> {
        Ok(Response::new(ProbeResponse { ready: Some(true) }))
    }
}

// ── Controller Service ──

#[tonic::async_trait]
impl controller_server::Controller for CsiDriver {
    async fn create_volume(
        &self,
        req: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let req = req.into_inner();
        let name = req.name.clone();
        let gib = requested_gib(req.capacity_range.as_ref());
        let size = format!("{gib}Gi");
        // With csi-provisioner's --extra-create-metadata the parameters carry
        // the RWX claim identity; the failover controller uses it to evict
        // workload pods. StorageClass parameters arrive here too.
        let pvc_name = req
            .parameters
            .get("csi.storage.k8s.io/pvc/name")
            .cloned()
            .unwrap_or_default();
        let pvc_namespace = req
            .parameters
            .get("csi.storage.k8s.io/pvc/namespace")
            .cloned()
            .unwrap_or_default();
        let backing_sc = req
            .parameters
            .get("backingStorageClass")
            .cloned()
            .unwrap_or_else(provision::backing_storage_class);

        info!(volume = %name, size = %size, backing_sc = %backing_sc, "CSI CreateVolume");

        let vn = volume_name(&name);
        let ns = provision::share_namespace();

        // Create backing PVC
        let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(self.kube.clone(), &ns);
        if pvc_api.get(&provision::backing_pvc_name(&vn)).await.is_err() {
            let backing: PersistentVolumeClaim =
                serde_json::from_value(provision::backing_pvc_json(&name, &size, &backing_sc))
                    .map_err(st)?;
            pvc_api.create(&Default::default(), &backing).await.map_err(st)?;
        }

        // Create service
        let svc_api: Api<Service> = Api::namespaced(self.kube.clone(), &ns);
        if svc_api.get(&vn).await.is_err() {
            let svc: Service =
                serde_json::from_value(provision::service_json(&name)).map_err(st)?;
            svc_api.create(&Default::default(), &svc).await.map_err(st)?;
        }

        // Create share-manager pod (or reuse existing)
        let pod_api: Api<Pod> = Api::namespaced(self.kube.clone(), &ns);
        let pod_name = provision::share_pod_name(&vn);

        // Check if pod exists — if it's Pending (stuck), delete and recreate
        if let Ok(existing_pod) = pod_api.get(&pod_name).await {
            let phase = existing_pod.status.as_ref().and_then(|s| s.phase.as_deref()).unwrap_or("");
            if phase == "Pending" {
                warn!(pod = %pod_name, "found stuck Pending share-manager, deleting");
                let _ = pod_api.delete(&pod_name, &DeleteParams::default()).await;
                // Wait for deletion
                for _ in 0..10 {
                    if pod_api.get(&pod_name).await.is_err() {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }

        if pod_api.get(&pod_name).await.is_err() {
            let node = provision::pick_node(&self.kube, &[]).await.map_err(st)?;
            let pod: Pod = serde_json::from_value(provision::share_pod_json(
                &name,
                &node,
                &pvc_name,
                &pvc_namespace,
            ))
            .map_err(st)?;
            pod_api.create(&Default::default(), &pod).await.map_err(st)?;
        }

        // Wait for pod running (max 30s — csi-provisioner has its own timeout)
        for _ in 0..15 {
            if let Ok(pod) = pod_api.get(&pod_name).await
                && pod.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running") {
                    let pod_ip = pod.status.as_ref().and_then(|s| s.pod_ip.as_deref()).unwrap_or("").to_string();
                    return Ok(Response::new(CreateVolumeResponse {
                        volume: Some(Volume {
                            volume_id: name.clone(),
                            capacity_bytes: gib * GIB,
                            volume_context: std::collections::HashMap::from([
                                ("nfsEndpoint".to_string(), format!("{pod_ip}:/{name}")),
                            ]),
                            content_source: None,
                            accessible_topology: vec![],
                        }),
                    }));
                }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }

        // Timeout — clean up resources so we don't leak pods/PVCs on retry.
        // The csi-provisioner will retry CreateVolume with a fresh attempt.
        warn!(volume = %name, "CreateVolume timeout, cleaning up share-manager pod + backing PVC");
        let _ = pod_api.delete(&pod_name, &DeleteParams::default()).await;
        let _ = pvc_api.delete(&provision::backing_pvc_name(&vn), &DeleteParams::default()).await;
        let _ = svc_api.delete(&vn, &DeleteParams::default()).await;
        Err(Status::deadline_exceeded("share-manager pod not ready in 30s"))
    }

    async fn delete_volume(
        &self,
        req: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let name = req.into_inner().volume_id;
        let vn = volume_name(&name);
        let ns = provision::share_namespace();
        info!(volume = %name, "CSI DeleteVolume");

        let pod_api: Api<Pod> = Api::namespaced(self.kube.clone(), &ns);
        let _ = pod_api.delete(&provision::share_pod_name(&vn), &DeleteParams::default()).await;
        let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(self.kube.clone(), &ns);
        let _ = pvc_api.delete(&provision::backing_pvc_name(&vn), &DeleteParams::default()).await;
        let svc_api: Api<Service> = Api::namespaced(self.kube.clone(), &ns);
        let _ = svc_api.delete(&vn, &DeleteParams::default()).await;
        let cm_api: Api<ConfigMap> = Api::namespaced(self.kube.clone(), &ns);
        let _ = cm_api.delete(&provision::state_cm_name(&vn), &DeleteParams::default()).await;

        Ok(Response::new(DeleteVolumeResponse {}))
    }

    async fn controller_publish_volume(
        &self, _req: Request<ControllerPublishVolumeRequest>,
    ) -> Result<Response<ControllerPublishVolumeResponse>, Status> {
        Ok(Response::new(ControllerPublishVolumeResponse { publish_context: Default::default() }))
    }

    async fn controller_unpublish_volume(
        &self, _req: Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<ControllerUnpublishVolumeResponse>, Status> {
        Ok(Response::new(ControllerUnpublishVolumeResponse {}))
    }

    async fn validate_volume_capabilities(
        &self, _req: Request<ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<ValidateVolumeCapabilitiesResponse>, Status> {
        Ok(Response::new(ValidateVolumeCapabilitiesResponse { confirmed: None, message: String::new() }))
    }

    async fn list_volumes(
        &self, _req: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        Ok(Response::new(ListVolumesResponse { entries: vec![], next_token: String::new() }))
    }

    async fn get_capacity(
        &self, _req: Request<GetCapacityRequest>,
    ) -> Result<Response<GetCapacityResponse>, Status> {
        Ok(Response::new(GetCapacityResponse { available_capacity: 1_099_511_627_776, maximum_volume_size: None, minimum_volume_size: None }))
    }

    async fn controller_get_capabilities(
        &self, _req: Request<ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<ControllerGetCapabilitiesResponse>, Status> {
        let mk = |t: i32| ControllerServiceCapability {
            r#type: Some(controller_service_capability::Type::Rpc(
                controller_service_capability::Rpc { r#type: t },
            )),
        };
        Ok(Response::new(ControllerGetCapabilitiesResponse {
            capabilities: vec![
                mk(controller_service_capability::rpc::Type::CreateDeleteVolume as i32),
                mk(controller_service_capability::rpc::Type::PublishUnpublishVolume as i32),
            ],
        }))
    }

    async fn create_snapshot(&self, _req: Request<CreateSnapshotRequest>) -> Result<Response<CreateSnapshotResponse>, Status> {
        Err(Status::unimplemented("snapshots not supported"))
    }
    async fn delete_snapshot(&self, _req: Request<DeleteSnapshotRequest>) -> Result<Response<DeleteSnapshotResponse>, Status> {
        Err(Status::unimplemented("snapshots not supported"))
    }
    async fn list_snapshots(&self, _req: Request<ListSnapshotsRequest>) -> Result<Response<ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented("snapshots not supported"))
    }
    async fn controller_expand_volume(&self, _req: Request<ControllerExpandVolumeRequest>) -> Result<Response<ControllerExpandVolumeResponse>, Status> {
        Err(Status::unimplemented("expansion not yet supported"))
    }
    async fn controller_get_volume(&self, _req: Request<ControllerGetVolumeRequest>) -> Result<Response<ControllerGetVolumeResponse>, Status> {
        Err(Status::unimplemented("controller_get_volume not supported"))
    }
    async fn controller_list_volume_health(&self, _req: Request<ControllerListVolumeHealthRequest>) -> Result<Response<ControllerListVolumeHealthResponse>, Status> {
        Err(Status::unimplemented("volume health not supported"))
    }
    async fn controller_get_volume_health(&self, _req: Request<ControllerGetVolumeHealthRequest>) -> Result<Response<ControllerGetVolumeHealthResponse>, Status> {
        Err(Status::unimplemented("volume health not supported"))
    }
    async fn get_snapshot(&self, _req: Request<GetSnapshotRequest>) -> Result<Response<GetSnapshotResponse>, Status> {
        Err(Status::unimplemented("snapshots not supported"))
    }
    async fn controller_modify_volume(&self, _req: Request<ControllerModifyVolumeRequest>) -> Result<Response<ControllerModifyVolumeResponse>, Status> {
        Err(Status::unimplemented("modify volume not supported"))
    }
}

// ── Node Service ──

/// Ask the share-manager's HTTP API for the volume's current NFS endpoint.
/// After a failover the endpoint in `volume_context` points at the dead node;
/// the share-manager service always resolves to the live one.
async fn resolve_current_endpoint(kube: &KubeClient, volume_id: &str) -> anyhow::Result<String> {
    let ns = provision::share_namespace();
    let svc_api: Api<Service> = Api::namespaced(kube.clone(), &ns);
    let svc = svc_api.get(&volume_name(volume_id)).await?;
    let cluster_ip = svc
        .spec
        .as_ref()
        .and_then(|s| s.cluster_ip.clone())
        .filter(|ip| !ip.is_empty() && ip != "None")
        .ok_or_else(|| anyhow::anyhow!("service has no cluster IP"))?;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;
    let resp: serde_json::Value = client
        .get(format!("http://{cluster_ip}:9500/endpoint"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    resp.get("endpoint")
        .and_then(|e| e.as_str())
        .filter(|e| !e.is_empty())
        .map(String::from)
        .ok_or_else(|| anyhow::anyhow!("share-manager reported no endpoint"))
}

#[tonic::async_trait]
impl node_server::Node for CsiDriver {
    async fn node_publish_volume(
        &self,
        req: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        let req = req.into_inner();
        let vol_id = req.volume_id.clone();
        let target = req.target_path.clone();

        let endpoint = match resolve_current_endpoint(&self.kube, &vol_id).await {
            Ok(ep) => ep,
            Err(e) => {
                warn!(volume = %vol_id, error = %e, "endpoint resolution failed");
                return Err(Status::unavailable("share-manager not ready, retry"));
            }
        };
        info!(volume = %vol_id, target = %target, endpoint = %endpoint, "CSI NodePublishVolume");

        std::fs::create_dir_all(&target).map_err(st)?;
        let server = endpoint.split(':').next().unwrap_or("");
        let path = endpoint.split(':').nth(1).unwrap_or("");
        if server.is_empty() || path.is_empty() {
            return Err(Status::invalid_argument(format!(
                "malformed NFS endpoint {endpoint:?}"
            )));
        }
        let status = std::process::Command::new("mount")
            .arg("-t").arg("nfs4")
            .arg("-o").arg("vers=4,hard,timeo=30,retrans=3")
            .arg(format!("{server}:{path}"))
            .arg(&target)
            .status().map_err(st)?;
        if !status.success() {
            return Err(Status::internal(format!("mount failed: {status}")));
        }
        Ok(Response::new(NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        req: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let target = req.into_inner().target_path;
        info!(target = %target, "CSI NodeUnpublishVolume");
        let status = std::process::Command::new("umount").arg(&target).status();
        if !matches!(status, Ok(s) if s.success()) {
            // Stale NFS mounts (dead server) block a regular umount forever.
            warn!(target = %target, "umount failed, retrying lazily");
            let _ = std::process::Command::new("umount").arg("-l").arg(&target).status();
        }
        Ok(Response::new(NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_capabilities(
        &self, _req: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        Ok(Response::new(NodeGetCapabilitiesResponse {
            capabilities: vec![NodeServiceCapability {
                r#type: Some(node_service_capability::Type::Rpc(
                    node_service_capability::Rpc {
                        r#type: node_service_capability::rpc::Type::StageUnstageVolume as i32,
                    },
                )),
            }],
        }))
    }

    async fn node_get_info(
        &self, _req: Request<NodeGetInfoRequest>,
    ) -> Result<Response<NodeGetInfoResponse>, Status> {
        let hostname = std::env::var("NODE_NAME").unwrap_or_else(|_| "unknown".into());
        Ok(Response::new(NodeGetInfoResponse {
            node_id: hostname, max_volumes_per_node: 0, accessible_topology: None,
        }))
    }

    async fn node_stage_volume(&self, _req: Request<NodeStageVolumeRequest>) -> Result<Response<NodeStageVolumeResponse>, Status> {
        Ok(Response::new(NodeStageVolumeResponse {}))
    }
    async fn node_unstage_volume(&self, _req: Request<NodeUnstageVolumeRequest>) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        Ok(Response::new(NodeUnstageVolumeResponse {}))
    }
    async fn node_get_volume_stats(&self, _req: Request<NodeGetVolumeStatsRequest>) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        Err(Status::unimplemented("volume stats not supported"))
    }
    async fn node_expand_volume(&self, _req: Request<NodeExpandVolumeRequest>) -> Result<Response<NodeExpandVolumeResponse>, Status> {
        Err(Status::unimplemented("expansion not yet supported"))
    }
    async fn node_get_volume_health(&self, _req: Request<NodeGetVolumeHealthRequest>) -> Result<Response<NodeGetVolumeHealthResponse>, Status> {
        Err(Status::unimplemented("volume health not supported"))
    }
    async fn node_get_storage_health(&self, _req: Request<NodeGetStorageHealthRequest>) -> Result<Response<NodeGetStorageHealthResponse>, Status> {
        Err(Status::unimplemented("storage health not supported"))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = Config::infer().await?;
    let kube = KubeClient::try_from(config)?;
    let endpoint = std::env::var("CSI_ENDPOINT")
        .unwrap_or_else(|_| "unix:///csi/csi.sock".into());

    info!(endpoint = %endpoint, "CSI driver starting");

    let driver = CsiDriver { kube };
    let svc = Server::builder()
        .add_service(identity_server::IdentityServer::new(driver.clone()))
        .add_service(controller_server::ControllerServer::new(driver.clone()))
        .add_service(node_server::NodeServer::new(driver));

    if let Some(path) = endpoint.strip_prefix("unix://") {
        let _ = std::fs::remove_file(path);
        let listener = tokio::net::UnixListener::bind(path)?;
        let incoming = UnixListenerStream::new(listener);
        info!(path = %path, "CSI gRPC server listening on unix socket");
        svc.serve_with_incoming(incoming).await?;
    } else {
        let addr: std::net::SocketAddr = endpoint.parse()?;
        info!(addr = %addr, "CSI gRPC server listening on TCP");
        svc.serve(addr).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requested_size_rounds_up_to_gib() {
        let range = |bytes: i64| CapacityRange { required_bytes: bytes, limit_bytes: 0 };
        assert_eq!(requested_gib(None), 10);
        assert_eq!(requested_gib(Some(&range(0))), 10);
        assert_eq!(requested_gib(Some(&range(1))), 1);
        assert_eq!(requested_gib(Some(&range(GIB))), 1);
        assert_eq!(requested_gib(Some(&range(GIB + 1))), 2);
        assert_eq!(requested_gib(Some(&range(15 * GIB))), 15);
    }
}
