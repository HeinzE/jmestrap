//! Control API — transport-agnostic recording control
//!
//! Defines the `RecordingControl` trait and request/response types.
//! Transport layers (e.g., REST) are thin adapters over this.

use crate::core::{AppState, RecordedEvent, RecordingInfo, RecordingRef, SourceStats, UntilProgress};
use crate::predicates::{Jmes, JmesUntil, UntilSpec};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

// =============================================================================
// Request types
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct StartRequest {
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub matching: Option<String>,
    #[serde(default)]
    pub until: Option<UntilSpec>,
}

#[derive(Debug, Deserialize)]
pub struct GetEventsRequest {
    pub reference: RecordingRef,
    /// How long to wait (in seconds) for the recording to finish.
    /// None or 0 means return immediately.
    #[serde(default)]
    pub timeout: Option<f64>,
}

// =============================================================================
// Response types
// =============================================================================

#[derive(Debug, Serialize)]
pub struct StartResponse {
    pub reference: RecordingRef,
}

#[derive(Debug, Serialize)]
pub struct StopResponse {
    pub reference: RecordingRef,
    pub event_count: usize,
}

#[derive(Debug, Serialize)]
pub struct EventsResponse {
    pub reference: RecordingRef,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,
    pub sources: Vec<String>,
    pub finished: bool,
    pub active: bool,
    pub event_count: usize,
    pub events: Vec<RecordedEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matching_expr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub until: Option<UntilProgress>,
    pub created_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_event_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
    pub events_evaluated: u64,
}

#[derive(Debug, Serialize)]
pub struct DeleteResponse {
    pub reference: RecordingRef,
}

#[derive(Debug, Serialize)]
pub struct ListResponse {
    pub recordings: Vec<RecordingInfo>,
}

#[derive(Debug, Serialize)]
pub struct SourcesResponse {
    pub sources: Vec<SourceStats>,
}

// =============================================================================
// Error type
// =============================================================================

#[derive(Debug, Clone, Copy)]
pub enum ControlErrorKind {
    NotFound,
    InvalidPredicate,
}

#[derive(Debug, Serialize)]
pub struct ControlError {
    #[serde(skip)]
    pub kind: ControlErrorKind,
    pub message: String,
}

impl std::fmt::Display for ControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ControlError {}

impl ControlError {
    pub fn invalid_predicate(msg: impl Into<String>) -> Self {
        Self {
            kind: ControlErrorKind::InvalidPredicate,
            message: msg.into(),
        }
    }

    pub fn not_found(reference: RecordingRef) -> Self {
        Self {
            kind: ControlErrorKind::NotFound,
            message: format!("Recording {} not found", reference),
        }
    }
}

// =============================================================================
// RecordingControl — the control API contract
// =============================================================================

/// Transport-agnostic control API for managing recordings.
///
/// Each method corresponds to a user-facing operation. Transport layers
/// (e.g., REST) deserialize into the request types, call these methods,
/// and serialize the responses.
pub trait RecordingControl {
    fn start(
        &self,
        req: StartRequest,
    ) -> impl std::future::Future<Output = Result<StartResponse, ControlError>> + Send;

    fn stop(
        &self,
        reference: RecordingRef,
    ) -> impl std::future::Future<Output = Result<StopResponse, ControlError>> + Send;

    fn get_events(
        &self,
        req: GetEventsRequest,
    ) -> impl std::future::Future<Output = Result<EventsResponse, ControlError>> + Send;

    fn delete(
        &self,
        reference: RecordingRef,
    ) -> impl std::future::Future<Output = Result<DeleteResponse, ControlError>> + Send;

    fn list(
        &self,
    ) -> impl std::future::Future<Output = Result<ListResponse, ControlError>> + Send;

    fn list_sources(
        &self,
    ) -> impl std::future::Future<Output = Result<SourcesResponse, ControlError>> + Send;
}

// =============================================================================
// Implementation on AppState
// =============================================================================

impl RecordingControl for AppState {
    async fn start(&self, req: StartRequest) -> Result<StartResponse, ControlError> {
        // Compile matching predicate
        let matching = match req.matching {
            Some(expr) => Some(
                Jmes::new(&expr)
                    .map_err(|e| {
                        ControlError::invalid_predicate(format!(
                            "Invalid matching predicate: {}",
                            e
                        ))
                    })?,
            ),
            None => None,
        };

        // Compile until condition
        let until = match req.until {
            Some(spec) => Some(
                JmesUntil::from_spec(spec)
                    .map_err(|e| {
                        ControlError::invalid_predicate(format!(
                            "Invalid until predicate: {}",
                            e
                        ))
                    })?,
            ),
            None => None,
        };

        let reference = self
            .start_recording(req.description, req.sources, matching, until)
            .await;

        Ok(StartResponse { reference })
    }

    async fn stop(&self, reference: RecordingRef) -> Result<StopResponse, ControlError> {
        match self.stop_recording(reference).await {
            Some(events) => Ok(StopResponse {
                reference,
                event_count: events.len(),
            }),
            None => Err(ControlError::not_found(reference)),
        }
    }

    async fn get_events(&self, req: GetEventsRequest) -> Result<EventsResponse, ControlError> {
        // Try to return immediately
        let snap = self
            .get_recording(req.reference)
            .await
            .ok_or_else(|| ControlError::not_found(req.reference))?;

        let should_wait =
            !snap.info.finished && req.timeout.is_some() && req.timeout.unwrap() > 0.0;

        if !should_wait {
            return Ok(events_response(snap));
        }

        // Wait for completion or timeout
        let duration = Duration::from_secs_f64(req.timeout.unwrap());
        let _ = tokio::time::timeout(duration, snap.notifier.notified()).await;

        // Re-read after wait (recording may have finished, or timed out)
        let snap = self
            .get_recording(req.reference)
            .await
            .ok_or_else(|| ControlError::not_found(req.reference))?;

        Ok(events_response(snap))
    }

    async fn delete(&self, reference: RecordingRef) -> Result<DeleteResponse, ControlError> {
        if self.delete_recording(reference).await {
            Ok(DeleteResponse { reference })
        } else {
            Err(ControlError::not_found(reference))
        }
    }

    async fn list(&self) -> Result<ListResponse, ControlError> {
        Ok(ListResponse {
            recordings: self.list_recordings().await,
        })
    }

    async fn list_sources(&self) -> Result<SourcesResponse, ControlError> {
        let mut stats_map: HashMap<String, SourceStats> = self
            .source_stats()
            .await
            .into_iter()
            .map(|s| (s.source.clone(), s))
            .collect();

        // Ensure shard-only sources (have recordings but no events yet) appear
        for s in self.known_sources().await {
            stats_map.entry(s.clone()).or_insert_with(|| SourceStats {
                source: s,
                event_count: 0,
                last_seen_ms: 0,
                active_recording_count: 0,
            });
        }

        // Compute active recording counts per source
        let recordings = self.list_recordings().await;
        let mut active_counts: HashMap<String, usize> = HashMap::new();
        let mut wildcard_active: usize = 0;
        for rec in &recordings {
            if rec.active {
                if rec.sources.is_empty() {
                    wildcard_active += 1;
                } else {
                    for source in &rec.sources {
                        *active_counts.entry(source.clone()).or_insert(0) += 1;
                    }
                }
            }
        }

        let mut sources: Vec<SourceStats> = stats_map
            .into_values()
            .map(|mut s| {
                s.active_recording_count =
                    active_counts.get(&s.source).copied().unwrap_or(0) + wildcard_active;
                s
            })
            .collect();
        sources.sort_by(|a, b| b.last_seen_ms.cmp(&a.last_seen_ms));
        Ok(SourcesResponse { sources })
    }
}

// =============================================================================
// Helpers
// =============================================================================

fn events_response(snap: crate::core::RecordingSnapshot) -> EventsResponse {
    EventsResponse {
        reference: snap.info.reference,
        description: snap.info.description,
        sources: snap.info.sources,
        finished: snap.info.finished,
        active: snap.info.active,
        event_count: snap.events.len(),
        matching_expr: snap.info.matching_expr,
        until: snap.info.until,
        events: snap.events,
        created_at_ms: snap.info.created_at_ms,
        first_event_at_ms: snap.info.first_event_at_ms,
        last_event_at_ms: snap.info.last_event_at_ms,
        finished_at_ms: snap.info.finished_at_ms,
        events_evaluated: snap.info.events_evaluated,
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_start_and_list() {
        let state = AppState::new();
        let resp = state
            .start(StartRequest {
                description: String::new(),
                sources: vec!["dut1".into()],

                matching: None,
                until: None,
            })
            .await
            .unwrap();

        assert_eq!(resp.reference, 1);

        let list = state.list().await.unwrap();
        assert_eq!(list.recordings.len(), 1);
        assert_eq!(list.recordings[0].reference, 1);
    }

    #[tokio::test]
    async fn test_description_persists_and_defaults_to_empty() {
        let state = AppState::new();

        // With description
        state
            .start(StartRequest {
                description: "login flow".into(),
                sources: vec!["dut1".into()],
                matching: None,
                until: None,
            })
            .await
            .unwrap();

        // Without description (default)
        state
            .start(StartRequest {
                description: String::new(),
                sources: vec!["dut2".into()],
                matching: None,
                until: None,
            })
            .await
            .unwrap();

        let list = state.list().await.unwrap();
        let rec1 = list.recordings.iter().find(|r| r.reference == 1).unwrap();
        let rec2 = list.recordings.iter().find(|r| r.reference == 2).unwrap();
        assert_eq!(rec1.description, "login flow");
        assert_eq!(rec2.description, "");

        // Description available on get_events too
        let events_resp = state
            .get_events(GetEventsRequest {
                reference: 1,
                timeout: None,
            })
            .await
            .unwrap();
        assert_eq!(events_resp.description, "login flow");
    }

    #[tokio::test]
    async fn test_start_invalid_matching() {
        let state = AppState::new();
        let err = state
            .start(StartRequest {
                description: String::new(),
                sources: vec![],

                matching: Some("bad predicate [[[".into()),
                until: None,
            })
            .await
            .unwrap_err();

        assert!(err.message.contains("Invalid matching predicate"));
    }

    #[tokio::test]
    async fn test_start_invalid_until() {
        let state = AppState::new();
        let err = state
            .start(StartRequest {
                description: String::new(),
                sources: vec![],

                matching: None,
                until: Some(UntilSpec::Order {
                    predicates: vec!["bad [[".into()],
                }),
            })
            .await
            .unwrap_err();

        assert!(err.message.contains("Invalid until predicate"));
    }

    #[tokio::test]
    async fn test_stop_existing() {
        let state = AppState::new();
        let r = state
            .start(StartRequest {
                description: String::new(),
                sources: vec![],

                matching: None,
                until: None,
            })
            .await
            .unwrap();

        let resp = state.stop(r.reference).await.unwrap();
        assert_eq!(resp.reference, 1);
        assert_eq!(resp.event_count, 0);
    }

    #[tokio::test]
    async fn test_stop_nonexistent() {
        let state = AppState::new();
        let err = state.stop(999).await.unwrap_err();
        assert!(err.message.contains("not found"));
    }

    #[tokio::test]
    async fn test_get_events_immediate_no_timeout() {
        let state = AppState::new();
        let r = state
            .start(StartRequest {
                description: String::new(),
                sources: vec![],

                matching: None,
                until: None,
            })
            .await
            .unwrap();

        let resp = state
            .get_events(GetEventsRequest {
                reference: r.reference,
                timeout: None,
            })
            .await
            .unwrap();

        assert!(!resp.finished);
        assert!(resp.events.is_empty());
    }

    #[tokio::test]
    async fn test_get_events_already_finished() {
        let state = AppState::new();

        // Start with until condition
        let r = state
            .start(StartRequest {
                description: String::new(),
                sources: vec![],

                matching: None,
                until: Some(UntilSpec::Order {
                    predicates: vec!["event == 'done'".into()],
                }),
            })
            .await
            .unwrap();

        // Inject the finishing event
        use crate::ingress::Event;
        use serde_json::json;
        state
            .process_event(&Event {
                source: "s".into(),
                payload: json!({"event": "done"}),
            })
            .await;

        // Get events with timeout — should return immediately since finished
        let resp = state
            .get_events(GetEventsRequest {
                reference: r.reference,
                timeout: Some(10.0),
            })
            .await
            .unwrap();

        assert!(resp.finished);
        assert_eq!(resp.events.len(), 1);
    }

    #[tokio::test]
    async fn test_get_events_timeout_expires() {
        let state = AppState::new();
        let r = state
            .start(StartRequest {
                description: String::new(),
                sources: vec![],

                matching: None,
                until: Some(UntilSpec::Order {
                    predicates: vec!["event == 'never'".into()],
                }),
            })
            .await
            .unwrap();

        // Short timeout, event never arrives
        let resp = state
            .get_events(GetEventsRequest {
                reference: r.reference,
                timeout: Some(0.1),
            })
            .await
            .unwrap();

        assert!(!resp.finished);
    }

    #[tokio::test]
    async fn test_get_events_wait_then_finish() {
        let state = Arc::new(AppState::new());

        let r = state
            .start(StartRequest {
                description: String::new(),
                sources: vec![],

                matching: None,
                until: Some(UntilSpec::Order {
                    predicates: vec!["event == 'done'".into()],
                }),
            })
            .await
            .unwrap();

        // Spawn a task to finish the recording after a short delay
        let state2 = Arc::clone(&state);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            use crate::ingress::Event;
            use serde_json::json;
            state2
                .process_event(&Event {
                    source: "s".into(),
                    payload: json!({"event": "done"}),
                })
                .await;
        });

        // Wait with generous timeout — should return when recording finishes
        let resp = state
            .get_events(GetEventsRequest {
                reference: r.reference,
                timeout: Some(5.0),
            })
            .await
            .unwrap();

        assert!(resp.finished);
        assert_eq!(resp.events.len(), 1);
    }

    #[tokio::test]
    async fn test_get_events_nonexistent() {
        let state = AppState::new();
        let err = state
            .get_events(GetEventsRequest {
                reference: 999,
                timeout: None,
            })
            .await
            .unwrap_err();

        assert!(err.message.contains("not found"));
    }

    #[tokio::test]
    async fn test_delete_existing() {
        let state = AppState::new();
        let r = state
            .start(StartRequest {
                description: String::new(),
                sources: vec![],

                matching: None,
                until: None,
            })
            .await
            .unwrap();

        state.delete(r.reference).await.unwrap();

        let list = state.list().await.unwrap();
        assert!(list.recordings.is_empty());
    }

    #[tokio::test]
    async fn test_delete_nonexistent() {
        let state = AppState::new();
        let err = state.delete(999).await.unwrap_err();
        assert!(err.message.contains("not found"));
    }

    // =========================================================================
    // Enriched list_sources
    // =========================================================================

    #[tokio::test]
    async fn test_list_sources_with_stats() {
        use crate::ingress::Event;

        let state = AppState::new();

        // Inject events (no recording needed — stats track all events)
        state.process_event(&Event {
            source: "src1".into(),
            payload: serde_json::json!({"v": 1}),
        }).await;
        state.process_event(&Event {
            source: "src1".into(),
            payload: serde_json::json!({"v": 2}),
        }).await;

        let resp = state.list_sources().await.unwrap();
        assert_eq!(resp.sources.len(), 1);
        let s = &resp.sources[0];
        assert_eq!(s.source, "src1");
        assert_eq!(s.event_count, 2);
        assert!(s.last_seen_ms > 0);
    }

    #[tokio::test]
    async fn test_list_sources_shard_only_sources_appear() {
        let state = AppState::new();

        // Start recording for "dut1" — creates shard, but no events yet
        state.start(StartRequest {
            description: String::new(),
            sources: vec!["dut1".into()],
            matching: None,
            until: None,
        }).await.unwrap();

        let resp = state.list_sources().await.unwrap();
        assert_eq!(resp.sources.len(), 1);
        assert_eq!(resp.sources[0].source, "dut1");
        assert_eq!(resp.sources[0].event_count, 0);
    }

    #[tokio::test]
    async fn test_list_sources_active_recording_count() {
        use crate::ingress::Event;

        let state = AppState::new();

        // Two recordings for src1, one for src2
        state.start(StartRequest {
            description: String::new(),
            sources: vec!["src1".into()],
            matching: None, until: None,
        }).await.unwrap();
        state.start(StartRequest {
            description: String::new(),
            sources: vec!["src1".into()],
            matching: None, until: None,
        }).await.unwrap();
        state.start(StartRequest {
            description: String::new(),
            sources: vec!["src2".into()],
            matching: None, until: None,
        }).await.unwrap();

        // Inject events so both sources appear in stats
        state.process_event(&Event {
            source: "src1".into(),
            payload: serde_json::json!({}),
        }).await;
        state.process_event(&Event {
            source: "src2".into(),
            payload: serde_json::json!({}),
        }).await;

        let resp = state.list_sources().await.unwrap();
        let s1 = resp.sources.iter().find(|s| s.source == "src1").unwrap();
        let s2 = resp.sources.iter().find(|s| s.source == "src2").unwrap();
        assert_eq!(s1.active_recording_count, 2);
        assert_eq!(s2.active_recording_count, 1);
    }

    #[tokio::test]
    async fn test_list_sources_wildcard_recording_counted() {
        use crate::ingress::Event;

        let state = AppState::new();

        // Wildcard recording (sources=[])
        state.start(StartRequest {
            description: String::new(),
            sources: vec![],
            matching: None, until: None,
        }).await.unwrap();

        // Inject event so src1 appears
        state.process_event(&Event {
            source: "src1".into(),
            payload: serde_json::json!({}),
        }).await;

        let resp = state.list_sources().await.unwrap();
        assert_eq!(resp.sources.len(), 1);
        // Wildcard recording counts for every source
        assert_eq!(resp.sources[0].active_recording_count, 1);
    }
}
