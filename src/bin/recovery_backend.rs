//! hcloud-csi-rwx NFSv4 Recovery Backend.
//!
//! Implements the Longhorn recovery-backend HTTP API that ganesha's
//! `hcloud` RecoveryBackend talks to. Stores NFSv4 client state in
//! ConfigMaps so that after share-manager failover the new ganesha
//! process can restore client locks/delegations during the grace period.
//!
//! API (matches longhorn-manager recovery_backend/server/router.go):
//!   POST   /v1/recoverybackend                      — create configmap
//!   PUT    /v1/recoverybackend/{hostname}            — end grace
//!   PUT    /v1/recoverybackend/{hostname}/{clientid}  — add client id
//!   DELETE /v1/recoverybackend/{hostname}/{clientid}  — remove client id
//!   GET    /v1/recoverybackend/{hostname}             — read client ids
//!   PUT    /v1/recoverybackend/{hostname}/{clientid}/{revokefh} — add revoke fh
//!
//! If the env var RECOVERY_BACKEND_TOKEN is set, all /v1/recoverybackend
//! routes require `Authorization: Bearer <token>`. /v1/healthz stays open
//! for probes.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::{Api, Patch, PatchParams};
use kube::{Client, Config};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

fn namespace() -> String {
    std::env::var("RECOVERY_BACKEND_NAMESPACE")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "hcloud-csi-rwx".into())
}

#[derive(Clone, Serialize, Deserialize)]
struct RecoveryBackendInput {
    hostname: String,
    version: String,
}

#[derive(Clone, Serialize)]
struct RecoveryBackendStatus {
    id: String,
    #[serde(rename = "type")]
    typ: String,
    hostname: String,
    clients: Vec<String>,
}

struct Ctx {
    client: Client,
}

type ApiError = (StatusCode, String);
type ApiResult<T> = Result<T, ApiError>;

fn bad_request(e: impl std::fmt::Display) -> ApiError {
    (StatusCode::BAD_REQUEST, e.to_string())
}

/// Map kube errors to the closest HTTP status so ganesha sees real failures
/// (it treats anything != 200 as an error).
fn kube_error(e: kube::Error) -> ApiError {
    let code = match &e {
        kube::Error::Api(resp) => {
            StatusCode::from_u16(resp.code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (code, e.to_string())
}

async fn create(State(ctx): State<Arc<Ctx>>, body: Bytes) -> ApiResult<StatusCode> {
    let input: RecoveryBackendInput = serde_json::from_slice(&body).map_err(bad_request)?;
    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &namespace());
    let name = cm_name(&input.hostname);
    let cm = ConfigMap {
        metadata: kube::api::ObjectMeta {
            name: Some(name.clone()),
            namespace: Some(namespace()),
            annotations: Some(BTreeMap::from([("version".to_string(), input.version.clone())])),
            ..Default::default()
        },
        data: Some(BTreeMap::from([(input.version.clone(), "{}".to_string())])),
        ..Default::default()
    };
    match cm_api.create(&Default::default(), &cm).await {
        Ok(_) => info!(hostname = %input.hostname, "created recovery configmap"),
        Err(kube::Error::Api(e)) if e.code == 409 => {
            // Already exists — update version
            let patch = serde_json::json!({
                "metadata": {"annotations": {"version": input.version}},
                "data": {input.version: "{}"}
            });
            cm_api
                .patch(&name, &PatchParams::apply("recovery-backend"), &Patch::Merge(&patch))
                .await
                .map_err(kube_error)?;
            info!(hostname = %input.hostname, "updated existing recovery configmap");
        }
        Err(e) => return Err(kube_error(e)),
    }
    Ok(StatusCode::OK)
}

async fn end_grace(
    State(ctx): State<Arc<Ctx>>,
    Path(hostname): Path<String>,
    body: Bytes,
) -> ApiResult<StatusCode> {
    let input: RecoveryBackendInput = serde_json::from_slice(&body).map_err(bad_request)?;
    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &namespace());
    let name = cm_name(&hostname);
    let cm = cm_api.get(&name).await.map_err(kube_error)?;
    let old_version = cm
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get("version"))
        .cloned()
        .unwrap_or_default();

    let mut annotations = cm.metadata.annotations.clone().unwrap_or_default();
    annotations.insert("version".to_string(), input.version.clone());
    let mut data = cm.data.clone().unwrap_or_default();
    data.remove(&old_version);
    data.entry(input.version).or_insert_with(|| "{}".to_string());

    let patch = serde_json::json!({
        "metadata": {"annotations": annotations},
        "data": data
    });
    cm_api
        .patch(&name, &PatchParams::apply("recovery-backend"), &Patch::Merge(&patch))
        .await
        .map_err(kube_error)?;
    info!(hostname = %hostname, "ended grace");
    Ok(StatusCode::OK)
}

async fn add_client_id(
    State(ctx): State<Arc<Ctx>>,
    Path((hostname, client_id)): Path<(String, String)>,
    body: Bytes,
) -> ApiResult<StatusCode> {
    let input: RecoveryBackendInput = serde_json::from_slice(&body).map_err(bad_request)?;
    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &namespace());
    let name = cm_name(&hostname);
    let cm = cm_api.get(&name).await.map_err(kube_error)?;
    let version = input.version;
    let data_str = cm
        .data
        .as_ref()
        .and_then(|d| d.get(&version))
        .cloned()
        .unwrap_or_default();
    let mut data: BTreeMap<String, Vec<String>> = serde_json::from_str(&data_str).unwrap_or_default();
    data.entry(client_id.clone()).or_default();
    let new_data = serde_json::to_string(&data).map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;

    let mut cm_data = cm.data.clone().unwrap_or_default();
    cm_data.insert(version, new_data);
    let patch = serde_json::json!({"data": cm_data});
    cm_api
        .patch(&name, &PatchParams::apply("recovery-backend"), &Patch::Merge(&patch))
        .await
        .map_err(kube_error)?;
    info!(hostname = %hostname, client_id = %client_id, "added client id");
    Ok(StatusCode::OK)
}

async fn remove_client_id(
    State(ctx): State<Arc<Ctx>>,
    Path((hostname, client_id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &namespace());
    let name = cm_name(&hostname);
    let cm = cm_api.get(&name).await.map_err(kube_error)?;
    let version = cm
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get("version"))
        .cloned()
        .unwrap_or_default();
    let data_str = cm
        .data
        .as_ref()
        .and_then(|d| d.get(&version))
        .cloned()
        .unwrap_or_default();
    let mut data: BTreeMap<String, Vec<String>> = serde_json::from_str(&data_str).unwrap_or_default();
    data.remove(&client_id);
    let new_data = serde_json::to_string(&data).map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;

    let mut cm_data = cm.data.clone().unwrap_or_default();
    cm_data.insert(version, new_data);
    let patch = serde_json::json!({"data": cm_data});
    cm_api
        .patch(&name, &PatchParams::apply("recovery-backend"), &Patch::Merge(&patch))
        .await
        .map_err(kube_error)?;
    info!(hostname = %hostname, client_id = %client_id, "removed client id");
    Ok(StatusCode::OK)
}

async fn read_client_ids(
    State(ctx): State<Arc<Ctx>>,
    Path(hostname): Path<String>,
) -> ApiResult<Json<RecoveryBackendStatus>> {
    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &namespace());
    let name = cm_name(&hostname);
    let cm = cm_api.get(&name).await.map_err(kube_error)?;
    let version = cm
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get("version"))
        .cloned()
        .unwrap_or_default();
    let data_str = cm
        .data
        .as_ref()
        .and_then(|d| d.get(&version))
        .cloned()
        .unwrap_or_default();
    let data: BTreeMap<String, Vec<String>> = serde_json::from_str(&data_str).unwrap_or_default();
    let clients: Vec<String> = data.keys().cloned().collect();
    info!(hostname = %hostname, clients = ?clients, "read client ids");
    Ok(Json(RecoveryBackendStatus {
        id: hostname.clone(),
        typ: "recoveryBackendStatus".into(),
        hostname,
        clients,
    }))
}

async fn add_revoke_fh(
    State(ctx): State<Arc<Ctx>>,
    Path((hostname, client_id, revoke_fh)): Path<(String, String, String)>,
    body: Bytes,
) -> ApiResult<StatusCode> {
    let input: RecoveryBackendInput = serde_json::from_slice(&body).map_err(bad_request)?;
    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &namespace());
    let name = cm_name(&hostname);
    let cm = cm_api.get(&name).await.map_err(kube_error)?;
    let version = input.version;
    let data_str = cm
        .data
        .as_ref()
        .and_then(|d| d.get(&version))
        .cloned()
        .unwrap_or_default();
    let mut data: BTreeMap<String, Vec<String>> = serde_json::from_str(&data_str).unwrap_or_default();
    data.entry(client_id.clone()).or_default().push(revoke_fh);
    let new_data = serde_json::to_string(&data).map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;

    let mut cm_data = cm.data.clone().unwrap_or_default();
    cm_data.insert(version, new_data);
    let patch = serde_json::json!({"data": cm_data});
    cm_api
        .patch(&name, &PatchParams::apply("recovery-backend"), &Patch::Merge(&patch))
        .await
        .map_err(kube_error)?;
    info!(hostname = %hostname, client_id = %client_id, "added revoke filehandle");
    Ok(StatusCode::OK)
}

fn cm_name(hostname: &str) -> String {
    format!("recovery-backend-{hostname}")
}

async fn healthz() -> &'static str {
    "ok"
}

/// Bearer-token check for all recovery routes. `token` is None when
/// RECOVERY_BACKEND_TOKEN is unset — auth disabled.
async fn require_bearer(
    State(token): State<Arc<Option<String>>>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    if let Some(expected) = token.as_ref() {
        let provided = req
            .headers()
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        if provided != Some(expected.as_str()) {
            return Err((StatusCode::UNAUTHORIZED, "invalid or missing token".into()));
        }
    }
    Ok(next.run(req).await)
}

fn recovery_routes(ctx: Arc<Ctx>, token: Arc<Option<String>>) -> Router {
    Router::new()
        .route("/v1/recoverybackend", post(create))
        .route("/v1/recoverybackend/{hostname}", put(end_grace).get(read_client_ids))
        .route(
            "/v1/recoverybackend/{hostname}/{clientid}",
            put(add_client_id).delete(remove_client_id),
        )
        .route("/v1/recoverybackend/{hostname}/{clientid}/{revokefh}", put(add_revoke_fh))
        .layer(middleware::from_fn_with_state(token, require_bearer))
        .with_state(ctx)
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
    let client = Client::try_from(config)?;
    let ctx = Arc::new(Ctx { client });

    let token = Arc::new(
        std::env::var("RECOVERY_BACKEND_TOKEN")
            .ok()
            .filter(|t| !t.is_empty()),
    );
    if token.is_none() {
        warn!("RECOVERY_BACKEND_TOKEN not set — recovery API runs without authentication");
    }

    let app = Router::new()
        .route("/v1/healthz", get(healthz))
        .merge(recovery_routes(ctx, token));

    let listen = std::env::var("RECOVERY_BACKEND_LISTEN").unwrap_or_else(|_| "0.0.0.0:9503".into());
    let listener = tokio::net::TcpListener::bind(&listen).await?;
    info!(addr = %listen, namespace = %namespace(), "recovery-backend listening");
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt;

    fn test_router(token: Option<String>) -> Router {
        // Auth middleware only — no kube client needed.
        Router::new()
            .route("/v1/recoverybackend/{hostname}", get(|| async { "ok" }))
            .layer(middleware::from_fn_with_state(Arc::new(token), require_bearer))
    }

    #[tokio::test]
    async fn no_token_configured_allows_requests() {
        let resp = test_router(None)
            .oneshot(
                HttpRequest::builder()
                    .uri("/v1/recoverybackend/host1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn wrong_or_missing_token_is_rejected() {
        let router = test_router(Some("s3cret".into()));
        let resp = router
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri("/v1/recoverybackend/host1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        let resp = router
            .oneshot(
                HttpRequest::builder()
                    .uri("/v1/recoverybackend/host1")
                    .header("authorization", "Bearer wrong")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn correct_token_is_accepted() {
        let resp = test_router(Some("s3cret".into()))
            .oneshot(
                HttpRequest::builder()
                    .uri("/v1/recoverybackend/host1")
                    .header("authorization", "Bearer s3cret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
