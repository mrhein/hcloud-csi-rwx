//! A fake Kubernetes API server for unit tests.
//!
//! `kube::Client::new` accepts any `tower::Service`, so we hand it one that
//! answers from a routing table instead of talking to a cluster. Tests can
//! assert on the requests that were made and script the responses — including
//! error codes — without any cluster, kubeconfig, or network.

use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use http::{Request, Response, StatusCode};
use kube::client::Body;
use serde_json::{json, Value};
use tower::Service;

/// One request the code under test made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seen {
    pub method: String,
    pub path: String,
    pub body: String,
}

impl Seen {
    pub fn summary(&self) -> String {
        format!("{} {}", self.method, self.path)
    }
}

type Responder = Box<dyn Fn(&Seen) -> Option<(StatusCode, Value)> + Send + Sync>;

/// Scriptable Kubernetes API double.
#[derive(Clone, Default)]
pub struct FakeApi {
    seen: Arc<Mutex<Vec<Seen>>>,
    routes: Arc<Mutex<Vec<Responder>>>,
    fallback: Arc<Mutex<Option<(StatusCode, Value)>>>,
}

impl std::fmt::Debug for FakeApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeApi").field("seen", &self.summaries()).finish()
    }
}

impl FakeApi {
    pub fn new() -> Self {
        Self::default()
    }

    /// Answer requests whose `"METHOD /path"` contains `needle` with `body`.
    pub fn on(self, needle: &str, status: StatusCode, body: Value) -> Self {
        let n = needle.to_string();
        self.routes.lock().unwrap().push(Box::new(move |s| {
            s.summary().contains(&n).then(|| (status, body.clone()))
        }));
        self
    }

    /// Answer matching requests with `200 OK` and `body`.
    pub fn ok(self, needle: &str, body: Value) -> Self {
        self.on(needle, StatusCode::OK, body)
    }

    /// Answer matching requests with a Kubernetes-style status error.
    pub fn err(self, needle: &str, status: StatusCode) -> Self {
        let reason = status.canonical_reason().unwrap_or("Error");
        self.on(
            needle,
            status,
            json!({
                "kind": "Status", "apiVersion": "v1", "status": "Failure",
                "message": format!("fake {reason}"), "reason": reason,
                "code": status.as_u16()
            }),
        )
    }

    /// Response for requests that match no route (default: 404).
    pub fn default_response(self, status: StatusCode, body: Value) -> Self {
        *self.fallback.lock().unwrap() = Some((status, body));
        self
    }

    /// Every request made so far.
    pub fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    /// `"METHOD /path"` for every request made so far.
    pub fn summaries(&self) -> Vec<String> {
        self.seen().iter().map(Seen::summary).collect()
    }

    /// True if any request's `"METHOD /path"` contains `needle`.
    pub fn called(&self, needle: &str) -> bool {
        self.summaries().iter().any(|s| s.contains(needle))
    }

    /// How many requests contain `needle`.
    pub fn count(&self, needle: &str) -> usize {
        self.summaries().iter().filter(|s| s.contains(needle)).count()
    }

    /// Bodies of requests whose summary contains `needle`.
    pub fn bodies(&self, needle: &str) -> Vec<String> {
        self.seen()
            .into_iter()
            .filter(|s| s.summary().contains(needle))
            .map(|s| s.body)
            .collect()
    }

    /// Build a `kube::Client` backed by this fake.
    pub fn client(&self) -> kube::Client {
        kube::Client::new(self.clone(), "hcloud-csi-rwx")
    }
}

impl Service<Request<Body>> for FakeApi {
    type Response = Response<Body>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let method = req.method().to_string();
        let path = req
            .uri()
            .path_and_query()
            .map(|p| p.to_string())
            .unwrap_or_else(|| req.uri().path().to_string());
        let seen_log = Arc::clone(&self.seen);
        let routes = Arc::clone(&self.routes);
        let fallback = Arc::clone(&self.fallback);

        Box::pin(async move {
            let body_bytes = http_body_util::BodyExt::collect(req.into_body())
                .await
                .map(|c| c.to_bytes())
                .unwrap_or_default();
            let entry = Seen {
                method,
                path,
                body: String::from_utf8_lossy(&body_bytes).into_owned(),
            };
            seen_log.lock().unwrap().push(entry.clone());

            let matched = routes.lock().unwrap().iter().find_map(|r| r(&entry));
            let (status, value) = matched
                .or_else(|| fallback.lock().unwrap().clone())
                .unwrap_or_else(|| {
                    (
                        StatusCode::NOT_FOUND,
                        json!({
                            "kind": "Status", "apiVersion": "v1", "status": "Failure",
                            "message": format!("fake: no route for {}", entry.summary()),
                            "reason": "NotFound", "code": 404
                        }),
                    )
                });

            Ok(Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&value).unwrap()))
                .unwrap())
        })
    }
}

// ── Object builders for common fixtures ──

/// A Node with the given Ready condition.
pub fn node(name: &str, ready: bool) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Node",
        "metadata": {"name": name},
        "status": {"conditions": [
            {"type": "Ready", "status": if ready {"True"} else {"False"}}
        ]}
    })
}

/// A `NodeList` of `(name, ready)` pairs.
pub fn node_list(nodes: &[(&str, bool)]) -> Value {
    json!({
        "apiVersion": "v1", "kind": "NodeList",
        "metadata": {"resourceVersion": "1"},
        "items": nodes.iter().map(|(n, r)| node(n, *r)).collect::<Vec<_>>()
    })
}

/// A share-manager Pod.
pub fn pod(name: &str, node_name: &str, phase: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "namespace": "hcloud-csi-rwx",
                     "labels": {"app": "hcloud-csi-rwx", "rwx-volume": "vol"}},
        "spec": {"nodeName": node_name},
        "status": {"phase": phase, "podIP": "10.255.0.1"}
    })
}

/// A `PodList`.
pub fn pod_list(pods: Vec<Value>) -> Value {
    json!({
        "apiVersion": "v1", "kind": "PodList",
        "metadata": {"resourceVersion": "1"},
        "items": pods
    })
}

/// An empty list of `kind`.
pub fn empty_list(kind: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": kind,
        "metadata": {"resourceVersion": "1"},
        "items": []
    })
}

/// A PersistentVolumeClaim.
pub fn pvc(name: &str, ns: &str, sc: &str, modes: &[&str], volume_name: Option<&str>) -> Value {
    let mut spec = json!({
        "storageClassName": sc,
        "accessModes": modes,
        "resources": {"requests": {"storage": "10Gi"}}
    });
    if let Some(v) = volume_name {
        spec["volumeName"] = json!(v);
    }
    json!({
        "apiVersion": "v1", "kind": "PersistentVolumeClaim",
        "metadata": {"name": name, "namespace": ns},
        "spec": spec,
        "status": {"phase": "Bound"}
    })
}

/// A ConfigMap holding share state.
pub fn state_cm(name: &str, state_json: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": name, "namespace": "hcloud-csi-rwx"},
        "data": {"state.json": state_json}
    })
}

/// A Service with a cluster IP.
pub fn service(name: &str, cluster_ip: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": name, "namespace": "hcloud-csi-rwx"},
        "spec": {"clusterIP": cluster_ip, "type": "ClusterIP"}
    })
}

/// A VolumeAttachment referencing `pv`.
pub fn volume_attachment(name: &str, pv: &str) -> Value {
    json!({
        "apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachment",
        "metadata": {"name": name},
        "spec": {"attacher": "csi.hetzner.cloud", "nodeName": "n1",
                 "source": {"persistentVolumeName": pv}},
        "status": {"attached": true}
    })
}

/// A `VolumeAttachmentList`.
pub fn va_list(items: Vec<Value>) -> Value {
    json!({
        "apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachmentList",
        "metadata": {"resourceVersion": "1"},
        "items": items
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::Node;
    use kube::api::{Api, ListParams};

    #[tokio::test]
    async fn fake_answers_list_and_records_request() {
        let fake = FakeApi::new().ok("GET /api/v1/nodes", node_list(&[("n1", true)]));
        let api: Api<Node> = Api::all(fake.client());
        let nodes = api.list(&ListParams::default()).await.unwrap();
        assert_eq!(nodes.items.len(), 1);
        assert!(fake.called("GET /api/v1/nodes"));
        assert_eq!(fake.count("GET /api/v1/nodes"), 1);
    }

    #[tokio::test]
    async fn fake_returns_scripted_errors() {
        let fake = FakeApi::new().err("GET /api/v1/nodes/missing", StatusCode::NOT_FOUND);
        let api: Api<Node> = Api::all(fake.client());
        let err = api.get("missing").await.unwrap_err();
        assert!(matches!(err, kube::Error::Api(e) if e.code == 404));
    }

    #[tokio::test]
    async fn unmatched_requests_get_404_by_default() {
        let fake = FakeApi::new();
        let api: Api<Node> = Api::all(fake.client());
        assert!(api.get("whatever").await.is_err());
        assert!(fake.called("GET /api/v1/nodes/whatever"));
    }

    #[tokio::test]
    async fn fallback_can_be_overridden() {
        let fake = FakeApi::new()
            .default_response(StatusCode::OK, node("n9", true));
        let api: Api<Node> = Api::all(fake.client());
        assert_eq!(api.get("anything").await.unwrap().metadata.name.unwrap(), "n9");
    }

    #[tokio::test]
    async fn bodies_are_captured() {
        let fake = FakeApi::new().ok("POST /api/v1/nodes", node("n1", true));
        let api: Api<Node> = Api::all(fake.client());
        let n: Node = serde_json::from_value(node("n1", true)).unwrap();
        let _ = api.create(&Default::default(), &n).await;
        let bodies = fake.bodies("POST /api/v1/nodes");
        assert!(bodies[0].contains("\"name\":\"n1\""), "got {bodies:?}");
    }

    #[test]
    fn builders_produce_expected_shapes() {
        assert_eq!(node("n", true)["status"]["conditions"][0]["status"], "True");
        assert_eq!(node("n", false)["status"]["conditions"][0]["status"], "False");
        assert_eq!(node_list(&[("a", true), ("b", false)])["items"].as_array().unwrap().len(), 2);
        assert_eq!(pod("p", "n", "Running")["status"]["phase"], "Running");
        assert_eq!(pod_list(vec![])["items"].as_array().unwrap().len(), 0);
        assert_eq!(empty_list("PodList")["kind"], "PodList");
        assert_eq!(pvc("c", "default", "sc", &["ReadWriteMany"], Some("pv1"))["spec"]["volumeName"], "pv1");
        assert!(pvc("c", "default", "sc", &["ReadWriteMany"], None)["spec"]["volumeName"].is_null());
        assert_eq!(state_cm("s", "{}")["data"]["state.json"], "{}");
        assert_eq!(service("s", "10.43.0.1")["spec"]["clusterIP"], "10.43.0.1");
        assert_eq!(volume_attachment("va", "pv1")["spec"]["source"]["persistentVolumeName"], "pv1");
        assert_eq!(va_list(vec![volume_attachment("va", "pv1")])["items"].as_array().unwrap().len(), 1);
        assert!(format!("{:?}", FakeApi::new()).contains("FakeApi"));
    }
}
