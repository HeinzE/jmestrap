//! REST transport layer (axum)
//!
//! Thin adapter: HTTP ↔ `RecordingControl` trait.
//! No business logic lives here.

use crate::control::{ControlError, ControlErrorKind, GetEventsRequest, RecordingControl, StartRequest};
use crate::core::{AppState, RecordingRef};
use crate::ingress::Event;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use tower_http::cors::CorsLayer;
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;

// =============================================================================
// Shared state alias
// =============================================================================

type AppStateRef = Arc<AppState>;

// =============================================================================
// ControlError → HTTP response mapping
// =============================================================================

impl IntoResponse for ControlError {
    fn into_response(self) -> axum::response::Response {
        let status = match self.kind {
            ControlErrorKind::NotFound => StatusCode::NOT_FOUND,
            ControlErrorKind::InvalidPredicate => StatusCode::UNPROCESSABLE_ENTITY,
        };
        (status, Json(json!({ "error": self.message }))).into_response()
    }
}

// =============================================================================
// Handlers
// =============================================================================

/// POST /recordings
async fn create_recording(
    State(state): State<AppStateRef>,
    Json(req): Json<StartRequest>,
) -> Result<impl IntoResponse, ControlError> {
    let resp = state.start(req).await?;
    Ok((StatusCode::CREATED, Json(resp)))
}

/// GET /recordings
async fn list_recordings(
    State(state): State<AppStateRef>,
) -> Result<impl IntoResponse, ControlError> {
    let resp = state.list().await?;
    Ok(Json(resp))
}

/// GET /recordings/:ref?timeout=N
#[derive(Deserialize)]
struct GetEventsQuery {
    #[serde(default)]
    timeout: Option<f64>,
}

async fn get_recording_events(
    State(state): State<AppStateRef>,
    Path(reference): Path<RecordingRef>,
    Query(query): Query<GetEventsQuery>,
) -> Result<impl IntoResponse, ControlError> {
    let resp = state
        .get_events(GetEventsRequest {
            reference,
            timeout: query.timeout,
        })
        .await?;
    Ok(Json(resp))
}

/// POST /recordings/:ref/stop
async fn stop_recording(
    State(state): State<AppStateRef>,
    Path(reference): Path<RecordingRef>,
) -> Result<impl IntoResponse, ControlError> {
    let resp = state.stop(reference).await?;
    Ok(Json(resp))
}

/// DELETE /recordings/:ref
async fn delete_recording(
    State(state): State<AppStateRef>,
    Path(reference): Path<RecordingRef>,
) -> Result<impl IntoResponse, ControlError> {
    let resp = state.delete(reference).await?;
    Ok(Json(resp))
}

/// GET /sources
async fn list_sources(
    State(state): State<AppStateRef>,
) -> Result<impl IntoResponse, ControlError> {
    let resp = state.list_sources().await?;
    Ok(Json(resp))
}

/// POST /events/:source — inject a single event
async fn inject_event(
    State(state): State<AppStateRef>,
    Path(source): Path<String>,
    Json(payload): Json<JsonValue>,
) -> impl IntoResponse {
    state
        .process_event(&Event {
            source: source.clone(),
            payload,
        })
        .await;
    (
        StatusCode::ACCEPTED,
        Json(json!({ "source": source })),
    )
}

/// GET /ping
async fn ping() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

// =============================================================================
// Router
// =============================================================================

pub fn router(state: AppStateRef) -> Router {
    let r = Router::new()
        .route("/ping", get(ping))
        .route("/recordings", post(create_recording).get(list_recordings))
        .route(
            "/recordings/{ref}",
            get(get_recording_events).delete(delete_recording),
        )
        .route("/recordings/{ref}/stop", post(stop_recording))
        .route("/sources", get(list_sources))
        .route("/events/{source}", post(inject_event));

    r.with_state(state).layer(CorsLayer::permissive())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use serde_json::json;
    use tower::util::ServiceExt;

    async fn request_json(
        app: &Router,
        req: Request<Body>,
    ) -> (axum::http::StatusCode, serde_json::Value) {
        let response = app.clone().oneshot(req).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = if bytes.is_empty() {
            json!({})
        } else {
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()
        };
        (status, body)
    }

    async fn request_raw(app: &Router, req: Request<Body>) -> (axum::http::StatusCode, String) {
        let response = app.clone().oneshot(req).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8_lossy(&bytes).to_string())
    }

    fn app() -> Router {
        router(Arc::new(AppState::new()))
    }

    #[tokio::test]
    async fn test_ping_route() {
        let app = app();
        let req = Request::builder()
            .method("GET")
            .uri("/ping")
            .body(Body::empty())
            .unwrap();

        let (status, body) = request_json(&app, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
    }

    #[tokio::test]
    async fn test_recording_lifecycle_routes() {
        let app = app();

        let create = Request::builder()
            .method("POST")
            .uri("/recordings")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"description": "boot check", "sources": ["dut1"], "matching": "event == 'boot'"}).to_string(),
            ))
            .unwrap();
        let (status, body) = request_json(&app, create).await;
        assert_eq!(status, StatusCode::CREATED);
        let reference = body["reference"].as_u64().unwrap();

        let list = Request::builder()
            .method("GET")
            .uri("/recordings")
            .body(Body::empty())
            .unwrap();
        let (status, body) = request_json(&app, list).await;
        assert_eq!(status, StatusCode::OK);
        let recordings = body["recordings"].as_array().unwrap();
        let rec = recordings
            .iter()
            .find(|r| r["reference"].as_u64() == Some(reference))
            .expect("recording in list");
        assert_eq!(rec["description"], "boot check");
        assert!(rec["created_at_ms"].as_u64().unwrap() > 0);

        let stop = Request::builder()
            .method("POST")
            .uri(format!("/recordings/{reference}/stop"))
            .body(Body::empty())
            .unwrap();
        let (status, body) = request_json(&app, stop).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["reference"], reference);

        let delete = Request::builder()
            .method("DELETE")
            .uri(format!("/recordings/{reference}"))
            .body(Body::empty())
            .unwrap();
        let (status, _) = request_json(&app, delete).await;
        assert_eq!(status, StatusCode::OK);

        let get_missing = Request::builder()
            .method("GET")
            .uri(format!("/recordings/{reference}"))
            .body(Body::empty())
            .unwrap();
        let (status, body) = request_json(&app, get_missing).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body["error"].as_str().unwrap().contains("not found"));
    }

    #[tokio::test]
    async fn test_invalid_matching_returns_422() {
        let app = app();
        let req = Request::builder()
            .method("POST")
            .uri("/recordings")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"sources": ["dut1"], "matching": "bad [[["}).to_string(),
            ))
            .unwrap();

        let (status, body) = request_json(&app, req).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("Invalid matching predicate")
        );
    }

    #[tokio::test]
    async fn test_inject_event_updates_sources_route() {
        let app = app();

        let inject = Request::builder()
            .method("POST")
            .uri("/events/station_a")
            .header("content-type", "application/json")
            .body(Body::from(json!({"event": "temp", "value": 21}).to_string()))
            .unwrap();
        let (status, _) = request_json(&app, inject).await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let sources = Request::builder()
            .method("GET")
            .uri("/sources")
            .body(Body::empty())
            .unwrap();
        let (status, body) = request_json(&app, sources).await;
        assert_eq!(status, StatusCode::OK);
        let sources = body["sources"].as_array().unwrap();
        let station = sources
            .iter()
            .find(|s| s["source"].as_str() == Some("station_a"))
            .unwrap();
        assert_eq!(station["event_count"], 1);
    }

    #[tokio::test]
    async fn test_get_recording_timeout_then_completion() {
        let app = app();

        let create = Request::builder()
            .method("POST")
            .uri("/recordings")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "sources": ["timeout_src"],
                    "until": { "type": "order", "predicates": ["event == 'go'"] }
                })
                .to_string(),
            ))
            .unwrap();
        let (status, body) = request_json(&app, create).await;
        assert_eq!(status, StatusCode::CREATED);
        let reference = body["reference"].as_u64().unwrap();

        let pre = Request::builder()
            .method("GET")
            .uri(format!("/recordings/{reference}?timeout=0.05"))
            .body(Body::empty())
            .unwrap();
        let (status, body) = request_json(&app, pre).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["finished"], false);
        // Timing fields present before any events
        assert!(body["created_at_ms"].is_u64(), "created_at_ms present");
        assert!(body["events_evaluated"].is_u64(), "events_evaluated present");
        assert!(body["first_event_at_ms"].is_null(), "no events yet");
        assert!(body["finished_at_ms"].is_null(), "not finished yet");

        let inject = Request::builder()
            .method("POST")
            .uri("/events/timeout_src")
            .header("content-type", "application/json")
            .body(Body::from(json!({"event": "go"}).to_string()))
            .unwrap();
        let (status, _) = request_json(&app, inject).await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let post = Request::builder()
            .method("GET")
            .uri(format!("/recordings/{reference}?timeout=0.2"))
            .body(Body::empty())
            .unwrap();
        let (status, body) = request_json(&app, post).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["finished"], true);
        // Timing fields populated after completion
        assert!(body["created_at_ms"].as_u64().unwrap() > 0);
        assert!(body["first_event_at_ms"].as_u64().unwrap() > 0);
        assert!(body["last_event_at_ms"].as_u64().unwrap() > 0);
        assert!(body["finished_at_ms"].as_u64().unwrap() > 0);
        assert!(body["events_evaluated"].as_u64().unwrap() >= 1);
        // Event itself carries timestamp
        let event = &body["events"][0];
        assert!(event["recorded_at_ms"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn test_invalid_json_body_returns_400() {
        let app = app();
        let req = Request::builder()
            .method("POST")
            .uri("/recordings")
            .header("content-type", "application/json")
            .body(Body::from("{not-json"))
            .unwrap();

        let (status, body) = request_raw(&app, req).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.to_lowercase().contains("json"));
    }

    #[tokio::test]
    async fn test_invalid_timeout_query_returns_400() {
        let app = app();

        let create = Request::builder()
            .method("POST")
            .uri("/recordings")
            .header("content-type", "application/json")
            .body(Body::from(json!({"sources": ["bad_timeout_src"]}).to_string()))
            .unwrap();
        let (status, body) = request_json(&app, create).await;
        assert_eq!(status, StatusCode::CREATED);
        let reference = body["reference"].as_u64().unwrap();

        let bad_timeout = Request::builder()
            .method("GET")
            .uri(format!("/recordings/{reference}?timeout=not-a-number"))
            .body(Body::empty())
            .unwrap();
        let (status, body) = request_raw(&app, bad_timeout).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.to_lowercase().contains("failed to deserialize"));
    }

    #[tokio::test]
    async fn test_stop_and_delete_missing_recording_return_404() {
        let app = app();

        let stop_missing = Request::builder()
            .method("POST")
            .uri("/recordings/999999/stop")
            .body(Body::empty())
            .unwrap();
        let (status, body) = request_json(&app, stop_missing).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body["error"].as_str().unwrap().contains("not found"));

        let delete_missing = Request::builder()
            .method("DELETE")
            .uri("/recordings/999999")
            .body(Body::empty())
            .unwrap();
        let (status, body) = request_json(&app, delete_missing).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body["error"].as_str().unwrap().contains("not found"));
    }

    #[tokio::test]
    async fn test_invalid_until_returns_422() {
        let app = app();
        let req = Request::builder()
            .method("POST")
            .uri("/recordings")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "sources": ["dut1"],
                    "until": {"type": "order", "predicates": ["bad [[["]}
                })
                .to_string(),
            ))
            .unwrap();

        let (status, body) = request_json(&app, req).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("Invalid until")
        );
    }
}
