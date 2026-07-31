use std::sync::Arc;

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use tokio::sync::RwLock;

/// Shared state exposed via the HTTP API.
/// The CSI controller plugin queries `/endpoint` to discover the NFS endpoint
/// for NodePublishVolume. The CSI node plugin queries `/healthz` for readiness.
#[derive(Clone, Serialize)]
pub struct ShareState {
    pub ready: bool,
    pub endpoint: Option<String>,
    pub volume: String,
    pub error: Option<String>,
}

pub type SharedState = Arc<RwLock<ShareState>>;

pub fn app(state: SharedState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/endpoint", get(endpoint))
        .route("/state", get(state_handler))
        .with_state(state)
}

async fn healthz(State(state): State<SharedState>) -> (axum::http::StatusCode, String) {
    let s = state.read().await;
    if s.ready {
        (axum::http::StatusCode::OK, "ok".into())
    } else {
        (axum::http::StatusCode::SERVICE_UNAVAILABLE, "not ready".into())
    }
}

async fn endpoint(State(state): State<SharedState>) -> (axum::http::StatusCode, Json<ShareState>) {
    let s = state.read().await;
    let code = if s.ready {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(s.clone()))
}

async fn state_handler(State(state): State<SharedState>) -> Json<ShareState> {
    let s = state.read().await;
    Json(s.clone())
}

/// Convenience to build a fresh state.
pub fn new_state(volume: &str) -> SharedState {
    Arc::new(RwLock::new(ShareState {
        ready: false,
        endpoint: None,
        volume: volume.into(),
        error: None,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    async fn get(state: SharedState, uri: &str) -> (StatusCode, serde_json::Value) {
        let resp = app(state)
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, json)
    }

    #[tokio::test]
    async fn healthz_flips_with_readiness() {
        let st = new_state("vol");
        let resp = app(st.clone())
            .oneshot(Request::builder().uri("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        st.write().await.ready = true;
        let resp = app(st)
            .oneshot(Request::builder().uri("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn endpoint_is_unavailable_until_ready() {
        let st = new_state("vol");
        let (status, _) = get(st.clone(), "/endpoint").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

        {
            let mut s = st.write().await;
            s.ready = true;
            s.endpoint = Some("10.0.0.1:/vol".into());
        }
        let (status, body) = get(st, "/endpoint").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["endpoint"], "10.0.0.1:/vol");
        assert_eq!(body["ready"], true);
    }

    #[tokio::test]
    async fn state_endpoint_always_answers() {
        let st = new_state("my-pvc");
        st.write().await.error = Some("boom".into());
        let (status, body) = get(st, "/state").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["volume"], "my-pvc");
        assert_eq!(body["error"], "boom");
        assert_eq!(body["ready"], false);
    }

    #[tokio::test]
    async fn fresh_state_has_sane_defaults() {
        let st = new_state("v");
        let s = st.read().await;
        assert!(!s.ready);
        assert!(s.endpoint.is_none());
        assert!(s.error.is_none());
        assert_eq!(s.volume, "v");
        // ShareState is Clone + Serialize
        let json = serde_json::to_string(&s.clone()).unwrap();
        assert!(json.contains("\"ready\":false"));
    }
}
