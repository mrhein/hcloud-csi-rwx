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
