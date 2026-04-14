//! Core state management for JMESTrap
//!
//! Transport-agnostic recording and event handling.

use crate::ingress::Event;
use crate::predicates::{Jmes, JmesUntil};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{RwLock, Notify};

// Alias to disambiguate std vs tokio RwLock
type StdRwLock<T> = std::sync::RwLock<T>;

// =============================================================================
// Types
// =============================================================================

pub type RecordingRef = u64;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// =============================================================================
// Source stats types
// =============================================================================

#[derive(Debug, Clone, Serialize)]
pub struct SourceStats {
    pub source: String,
    pub event_count: u64,
    pub last_seen_ms: u64,
    pub active_recording_count: usize,
}

struct SourceStatsInner {
    event_count: u64,
    last_seen_ms: u64,
}

// =============================================================================
// Predicate progress types
// =============================================================================

#[derive(Debug, Clone, Serialize)]
pub struct PredicateProgress {
    pub index: usize,
    pub expr: String,
    pub matched: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct UntilProgress {
    #[serde(rename = "type")]
    pub r#type: String,
    pub predicates: Vec<PredicateProgress>,
}

/// Explicit recording lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordingStatus {
    /// Actively accepting events
    Running,
    /// Until condition was satisfied
    Completed,
    /// Manually stopped
    Stopped,
}

/// Information about a recording (for API responses).
///
/// Used by both list and get-single endpoints. The get-single
/// response adds the `events` array alongside this metadata.
#[derive(Debug, Clone, Serialize)]
pub struct RecordingInfo {
    pub reference: RecordingRef,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub description: String,
    pub sources: Vec<String>,
    pub event_count: usize,
    pub status: RecordingStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matching_expr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub until: Option<UntilProgress>,
    /// Unix epoch ms when the recording was created.
    pub created_at_ms: u64,
    /// Unix epoch ms when the first event was recorded (None if no events yet).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_event_at_ms: Option<u64>,
    /// Unix epoch ms when the most recent event was recorded (None if no events yet).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_at_ms: Option<u64>,
    /// Unix epoch ms when the recording finished (None if still active).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
    /// Number of events evaluated against this recording (including non-matches).
    pub events_evaluated: u64,
}

/// A recorded event with envelope metadata.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RecordedEvent {
    pub source: String,
    pub payload: JsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub until_predicate: Option<usize>,
    /// Unix epoch milliseconds when this event was recorded.
    pub recorded_at_ms: u64,
}

/// Full recording state snapshot (for get_recording with events + metadata).
pub struct RecordingSnapshot {
    pub info: RecordingInfo,
    pub events: Vec<Arc<RecordedEvent>>,
    pub notifier: Arc<Notify>,
}

// =============================================================================
// Recording
// =============================================================================

/// A single recording session with JMESPath filtering
pub struct Recording {
    pub reference: RecordingRef,
    /// User-facing label for this recording (optional, empty = no description).
    pub description: String,
    /// Event-sources this recording subscribes to
    pub sources: Vec<String>,
    pub events: Vec<Arc<RecordedEvent>>,
    pub status: RecordingStatus,
    /// Optional filter predicate (what to record)
    matching: Option<Jmes>,
    /// Optional completion pattern (when to stop)
    until: Option<JmesUntil>,
    /// Notifier for when recording finishes (for fetch_recording with timeout)
    finished_notify: Arc<Notify>,
    /// Timestamps and counters
    pub created_at_ms: u64,
    pub first_event_at_ms: Option<u64>,
    pub last_event_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    pub events_evaluated: u64,
    /// Last time a client fetched this recording (atomic for read-lock updates)
    last_accessed_at: AtomicU64,
}

impl Recording {
    pub fn new(
        reference: RecordingRef,
        description: String,
        sources: Vec<String>,
        matching: Option<Jmes>,
        until: Option<JmesUntil>,
    ) -> Self {
        Self {
            reference,
            description,
            sources,
            events: Vec::new(),
            status: RecordingStatus::Running,
            matching,
            until,
            finished_notify: Arc::new(Notify::new()),
            created_at_ms: now_ms(),
            first_event_at_ms: None,
            last_event_at_ms: None,
            finished_at_ms: None,
            events_evaluated: 0,
            last_accessed_at: AtomicU64::new(now_ms()),
        }
    }

    /// Check if this recording should receive events from the given source
    pub fn matches_source(&self, source: &str) -> bool {
        self.sources.is_empty() || self.sources.iter().any(|s| s == source)
    }

    /// Evaluate an event against this recording's filter and completion pattern.
    /// Returns true if the recording has finished (completion pattern satisfied).
    pub fn evaluate_event(&mut self, source: &str, payload: &JsonValue) -> bool {
        if self.status != RecordingStatus::Running {
            return self.status == RecordingStatus::Completed;
        }

        self.events_evaluated += 1;

        // Check completion pattern first
        let until_match = self.until.as_mut().and_then(|u| u.try_match(payload));
        if let Some(predicate_index) = until_match {
            self.push_event(source, payload, Some(predicate_index));

            if self.until.as_ref().unwrap().is_complete() {
                self.status = RecordingStatus::Completed;
                self.finished_at_ms = Some(now_ms());
                self.finished_notify.notify_waiters();
                return true;
            }
            return false;
        }

        // Check filter predicate (None = record nothing, only completion pattern events)
        let should_record = match self.matching {
            Some(ref matching) => matching.is_match(payload),
            None => false,
        };
        if should_record {
            self.push_event(source, payload, None);
        }

        false
    }

    /// Push a recorded event and update timing metadata.
    fn push_event(&mut self, source: &str, payload: &JsonValue, until_predicate: Option<usize>) {
        let ts = now_ms();
        if self.first_event_at_ms.is_none() {
            self.first_event_at_ms = Some(ts);
        }
        self.last_event_at_ms = Some(ts);
        self.events.push(Arc::new(RecordedEvent {
            source: source.to_string(),
            payload: payload.clone(),
            until_predicate,
            recorded_at_ms: ts,
        }));
    }

    /// Get a handle to wait for recording to finish (for fetch_recording with timeout)
    pub fn finished_notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.finished_notify)
    }

    /// Get the matching expression string (if any)
    pub fn matching_expr(&self) -> Option<&str> {
        self.matching.as_ref().map(|m| m.expression())
    }

    /// Build a RecordingInfo summary of this recording.
    pub fn info(&self) -> RecordingInfo {
        RecordingInfo {
            reference: self.reference,
            description: self.description.clone(),
            sources: self.sources.clone(),
            event_count: self.events.len(),
            status: self.status,
            matching_expr: self.matching_expr().map(|s| s.to_string()),
            until: self.until_progress(),
            created_at_ms: self.created_at_ms,
            first_event_at_ms: self.first_event_at_ms,
            last_event_at_ms: self.last_event_at_ms,
            finished_at_ms: self.finished_at_ms,
            events_evaluated: self.events_evaluated,
        }
    }

    /// Get until predicate progress (if any)
    pub fn until_progress(&self) -> Option<UntilProgress> {
        self.until.as_ref().map(|u| {
            let type_name = match u {
                JmesUntil::Order(_) => "order",
                JmesUntil::AnyOrder(_) => "any_order",
            };
            let expressions = u.expressions();
            let matched = u.matched_status();
            let predicates: Vec<PredicateProgress> = expressions
                .iter()
                .zip(matched.iter())
                .enumerate()
                .map(|(i, (expr, &matched))| PredicateProgress {
                    index: i + 1,
                    expr: expr.to_string(),
                    matched,
                })
                .collect();
            UntilProgress {
                r#type: type_name.to_string(),
                predicates,
            }
        })
    }
}

// =============================================================================
// Recordings Manager
// =============================================================================

/// Manages all recordings
pub struct Recordings {
    recordings: HashMap<RecordingRef, Recording>,
}

impl Recordings {
    pub fn new() -> Self {
        Self {
            recordings: HashMap::new(),
        }
    }

    /// Insert a recording with a pre-allocated reference (used by AppState sharding)
    pub fn insert(&mut self, recording: Recording) {
        self.recordings.insert(recording.reference, recording);
    }

    pub fn stop(&mut self, reference: RecordingRef) -> Option<Vec<Arc<RecordedEvent>>> {
        if let Some(rec) = self.recordings.get_mut(&reference) {
            if rec.status == RecordingStatus::Running {
                rec.status = RecordingStatus::Stopped;
                rec.finished_at_ms = Some(now_ms());
                rec.finished_notify.notify_waiters();
            }
            Some(rec.events.clone())
        } else {
            None
        }
    }

    pub fn delete(&mut self, reference: RecordingRef) -> bool {
        if let Some(rec) = self.recordings.remove(&reference) {
            rec.finished_notify.notify_waiters();
            true
        } else {
            false
        }
    }

    pub fn get(&self, reference: RecordingRef) -> Option<&Recording> {
        self.recordings.get(&reference)
    }

    #[allow(dead_code)] // Test-only helper; runtime uses AppState::get_events()
    pub fn get_events(&self, reference: RecordingRef) -> Option<Vec<Arc<RecordedEvent>>> {
        self.recordings.get(&reference).map(|r| r.events.clone())
    }

    pub fn list(&self) -> Vec<RecordingInfo> {
        self.recordings.values().map(|r| r.info()).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.recordings.is_empty()
    }

    /// Get all active recordings that match a source
    pub fn active_for_source(&mut self, source: &str) -> Vec<&mut Recording> {
        self.recordings
            .values_mut()
            .filter(|r| r.status == RecordingStatus::Running && r.matches_source(source))
            .collect()
    }
}

impl Default for Recordings {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// AppState — source-sharded recording management
// =============================================================================

/// Application state shared across all connections.
///
/// Recordings are sharded by event-source: an event from source "dut1" only
/// locks the "dut1" shard, so events from "dut2" never contend.
///
/// Recordings with `sources = []` (match all) go into a global shard that
/// every event checks.  This is discouraged in production but useful for
/// diagnostics.
pub struct AppState {
    /// Per-source shards: source name → recordings interested in that source
    shards: RwLock<HashMap<String, Arc<RwLock<Recordings>>>>,

    /// Global shard: recordings with sources=[] (match all events)
    global: Arc<RwLock<Recordings>>,

    /// Ref counter (global, atomic — refs are unique across all shards)
    counter: std::sync::atomic::AtomicU64,

    /// Index: ref → list of source names (or empty vec for global)
    /// Needed to find which shard(s) own a recording for stop/get/delete.
    ref_index: RwLock<HashMap<RecordingRef, Vec<String>>>,

    /// Per-source event statistics, sharded like recordings.
    /// Outer RwLock: read-locked for per-event updates (finds existing source),
    /// write-locked only when a new source appears (rare).
    /// Inner Mutex: per-source, so events from different sources never contend.
    source_stats: StdRwLock<HashMap<String, std::sync::Mutex<SourceStatsInner>>>,

    /// TTL for recordings in milliseconds. Finished recordings are deleted
    /// after this duration; idle Running recordings are deleted if not accessed
    /// within this window.
    ttl_ms: u64,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            shards: RwLock::new(HashMap::new()),
            global: Arc::new(RwLock::new(Recordings::new())),
            counter: std::sync::atomic::AtomicU64::new(0),
            ref_index: RwLock::new(HashMap::new()),
            source_stats: StdRwLock::new(HashMap::new()),
            ttl_ms: 3_600_000,
        }
    }

    /// Override the default TTL (in seconds) for recording cleanup.
    pub fn with_ttl_secs(mut self, secs: u64) -> Self {
        self.ttl_ms = secs * 1000;
        self
    }

    /// Allocate a globally unique recording reference
    fn next_ref(&self) -> RecordingRef {
        self.counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1
    }

    /// Get or create a shard for the given source
    async fn shard_for(&self, source: &str) -> Arc<RwLock<Recordings>> {
        // Fast path: shard already exists
        {
            let shards = self.shards.read().await;
            if let Some(shard) = shards.get(source) {
                return Arc::clone(shard);
            }
        }
        // Slow path: create shard
        let mut shards = self.shards.write().await;
        // Double-check after acquiring write lock
        if let Some(shard) = shards.get(source) {
            return Arc::clone(shard);
        }
        let shard = Arc::new(RwLock::new(Recordings::new()));
        shards.insert(source.to_string(), Arc::clone(&shard));
        shard
    }

    /// Start a recording, placing it in the appropriate shard.
    ///
    /// - Exactly one source → goes into that source's shard (fast path)
    /// - Zero or multiple sources → goes into the global shard
    ///
    /// The common case (single DUT) hits the fast path.  Multi-source
    /// recordings are rare and handled correctly via global.
    pub async fn start_recording(
        &self,
        description: String,
        sources: Vec<String>,
        matching: Option<Jmes>,
        until: Option<JmesUntil>,
    ) -> RecordingRef {
        let reference = self.next_ref();
        let recording = Recording::new(reference, description, sources.clone(), matching, until);

        if sources.len() == 1 {
            // Fast path: single source → dedicated shard
            let source = &sources[0];
            let shard = self.shard_for(source).await;
            let mut shard = shard.write().await;
            shard.insert(recording);
            let mut index = self.ref_index.write().await;
            index.insert(reference, sources);
        } else {
            // Global: zero sources (match all) or multiple sources
            let mut global = self.global.write().await;
            global.insert(recording);
            let mut index = self.ref_index.write().await;
            index.insert(reference, vec![]);
        }

        reference
    }

    /// Find a recording by reference across all shards.
    /// Returns the shard's Recordings lock and whether it's global.
    async fn find_recording_shards(
        &self,
        reference: RecordingRef,
    ) -> Option<Vec<Arc<RwLock<Recordings>>>> {
        let index = self.ref_index.read().await;
        let sources = index.get(&reference)?;
        if sources.is_empty() {
            Some(vec![Arc::clone(&self.global)])
        } else {
            let shards = self.shards.read().await;
            let result: Vec<_> = sources
                .iter()
                .filter_map(|s| shards.get(s).map(Arc::clone))
                .collect();
            if result.is_empty() {
                None
            } else {
                Some(result)
            }
        }
    }

    /// Stop a recording. Returns events if found.
    pub async fn stop_recording(
        &self,
        reference: RecordingRef,
    ) -> Option<Vec<Arc<RecordedEvent>>> {
        let shard_locks = self.find_recording_shards(reference).await?;
        // Stop in first shard that has it (for shared recordings, stop once
        // and the Arc-shared state is visible to all shards)
        for shard_lock in &shard_locks {
            let mut shard = shard_lock.write().await;
            if let Some(events) = shard.stop(reference) {
                return Some(events);
            }
        }
        None
    }

    /// Get a recording's full state (metadata, events, notifier).
    pub async fn get_recording(
        &self,
        reference: RecordingRef,
    ) -> Option<RecordingSnapshot> {
        let shard_locks = self.find_recording_shards(reference).await?;
        for shard_lock in &shard_locks {
            let shard = shard_lock.read().await;
            if let Some(rec) = shard.get(reference) {
                // Touch: reset the idle-reaper clock (atomic — no write lock needed)
                rec.last_accessed_at.store(now_ms(), Ordering::Relaxed);
                return Some(RecordingSnapshot {
                    info: rec.info(),
                    events: rec.events.clone(),
                    notifier: rec.finished_notifier(),
                });
            }
        }
        None
    }

    /// Delete a recording from all shards.
    pub async fn delete_recording(&self, reference: RecordingRef) -> bool {
        let shard_locks = match self.find_recording_shards(reference).await {
            Some(s) => s,
            None => return false,
        };
        let mut deleted = false;
        for shard_lock in &shard_locks {
            let mut shard = shard_lock.write().await;
            if shard.delete(reference) {
                deleted = true;
            }
        }
        if deleted {
            let mut index = self.ref_index.write().await;
            index.remove(&reference);
        }
        deleted
    }

    /// List all recordings across all shards.
    pub async fn list_recordings(&self) -> Vec<RecordingInfo> {
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();

        // Global shard
        {
            let global = self.global.read().await;
            for info in global.list() {
                if seen.insert(info.reference) {
                    result.push(info);
                }
            }
        }

        // Per-source shards
        {
            let shards = self.shards.read().await;
            for shard_lock in shards.values() {
                let shard = shard_lock.read().await;
                for info in shard.list() {
                    if seen.insert(info.reference) {
                        result.push(info);
                    }
                }
            }
        }

        result.sort_by(|a, b| b.reference.cmp(&a.reference));
        result
    }

    /// Return all known event-source names (from shard keys).
    pub async fn known_sources(&self) -> Vec<String> {
        let shards = self.shards.read().await;
        shards.keys().cloned().collect()
    }

    /// Return aggregated source statistics
    pub async fn source_stats(&self) -> Vec<SourceStats> {
        let stats = self.source_stats.read().unwrap_or_else(|e| e.into_inner());
        stats
            .iter()
            .map(|(source, mutex)| {
                let inner = mutex.lock().unwrap_or_else(|e| e.into_inner());
                SourceStats {
                    source: source.clone(),
                    event_count: inner.event_count,
                    last_seen_ms: inner.last_seen_ms,
                    active_recording_count: 0,
                }
            })
            .collect()
    }

    /// Process an event: route to the source shard + global shard.
    pub async fn process_event(&self, event: &Event) {
        // Source-specific shard
        {
            let shards = self.shards.read().await;
            if let Some(shard_lock) = shards.get(&event.source) {
                let mut shard = shard_lock.write().await;
                let matching = shard.active_for_source(&event.source);
                for recording in matching {
                    let finished = recording.evaluate_event(&event.source, &event.payload);
                    if finished {
                        eprintln!(
                            "[recording/{}] Until condition satisfied",
                            recording.reference
                        );
                    }
                }
            }
        }

        // Global shard (recordings with sources=[])
        {
            let mut global = self.global.write().await;
            let matching = global.active_for_source(&event.source);
            for recording in matching {
                let finished = recording.evaluate_event(&event.source, &event.payload);
                if finished {
                    eprintln!(
                        "[recording/{}] Until condition satisfied",
                        recording.reference
                    );
                }
            }
        }

        // Update source stats (per-source lock, no cross-source contention)
        {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            // Fast path: read lock on outer map, per-source Mutex on inner
            {
                let stats = self.source_stats.read().unwrap_or_else(|e| e.into_inner());
                if let Some(mutex) = stats.get(&event.source) {
                    let mut entry = mutex.lock().unwrap_or_else(|e| e.into_inner());
                    entry.event_count += 1;
                    entry.last_seen_ms = now_ms;
                    return; // done — fast path
                }
            }
            // Slow path: new source, need write lock to insert
            {
                let mut stats = self.source_stats.write().unwrap_or_else(|e| e.into_inner());
                let mutex = stats
                    .entry(event.source.clone())
                    .or_insert_with(|| std::sync::Mutex::new(SourceStatsInner {
                        event_count: 0,
                        last_seen_ms: 0,
                    }));
                let mut entry = (*mutex).lock().unwrap_or_else(|e| e.into_inner());
                entry.event_count += 1;
                entry.last_seen_ms = now_ms;
            }
        }
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Event Processor Task
// =============================================================================

use crate::ingress::EventIngress;

/// Spawn a task that reads from an ingress and processes events
#[allow(dead_code)] // Called from feature-gated ingress startup (sse, mqtt) and tests
pub fn spawn_ingress_processor(
    mut ingress: Box<dyn EventIngress>,
    state: Arc<AppState>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = ingress.recv().await {
            state.process_event(&event).await;
        }
        println!("[ingress] Event source closed");
    })
}

// =============================================================================
// Recording Reaper
// =============================================================================

/// Spawn a background task that periodically deletes expired recordings.
pub fn spawn_reaper(
    state: Arc<AppState>,
    interval_secs: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(
            std::time::Duration::from_secs(interval_secs),
        );
        loop {
            ticker.tick().await;
            reap_expired(&state, now_ms()).await;
        }
    })
}

/// Delete recordings that have outlived their TTL.
///
/// - Completed/Stopped: expired when `now - finished_at_ms >= ttl`
/// - Running: expired when `now - last_accessed_at >= ttl`
///
/// Accepts `now` as a parameter so tests can control time.
pub(crate) async fn reap_expired(state: &AppState, now: u64) {
    let ttl = state.ttl_ms;
    let mut expired: Vec<RecordingRef> = Vec::new();

    // Scan global shard
    {
        let global = state.global.read().await;
        collect_expired(&global, ttl, now, &mut expired);
    }

    // Scan per-source shards
    {
        let shards = state.shards.read().await;
        for shard_lock in shards.values() {
            let shard = shard_lock.read().await;
            collect_expired(&shard, ttl, now, &mut expired);
        }
    }

    // Delete expired recordings
    for ref_id in &expired {
        state.delete_recording(*ref_id).await;
    }

    // Prune empty per-source shards
    {
        let shards = state.shards.read().await;
        let empty_keys: Vec<String> = {
            let mut empties = Vec::new();
            for (key, shard_lock) in shards.iter() {
                let shard = shard_lock.read().await;
                if shard.is_empty() {
                    empties.push(key.clone());
                }
            }
            empties
        };
        if !empty_keys.is_empty() {
            drop(shards); // release read lock before taking write lock
            let mut shards = state.shards.write().await;
            for key in &empty_keys {
                // Double-check: another task may have inserted into the shard
                if let Some(shard_lock) = shards.get(key) {
                    let shard = shard_lock.read().await;
                    if shard.is_empty() {
                        drop(shard);
                        shards.remove(key);
                    }
                }
            }
        }
    }

    if !expired.is_empty() {
        eprintln!("[reaper] Cleaned up {} expired recording(s)", expired.len());
    }
}

fn collect_expired(
    recordings: &Recordings,
    ttl: u64,
    now: u64,
    out: &mut Vec<RecordingRef>,
) {
    for rec in recordings.recordings.values() {
        let age = match rec.status {
            RecordingStatus::Running => {
                now.saturating_sub(rec.last_accessed_at.load(Ordering::Relaxed))
            }
            RecordingStatus::Completed | RecordingStatus::Stopped => {
                match rec.finished_at_ms {
                    Some(t) => now.saturating_sub(t),
                    None => continue, // shouldn't happen, but be safe
                }
            }
        };
        if age >= ttl {
            out.push(rec.reference);
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingress::{MockConfig, MockIngress, MockPattern, MockSource};
    use serde_json::json;

    #[tokio::test]
    async fn test_recording_matches_source() {
        let rec = Recording::new(
            1,
            String::new(),
            vec!["dut1".to_string()],
            None,
            None,
        );

        assert!(rec.matches_source("dut1"));
        assert!(!rec.matches_source("dut2"));
    }

    #[tokio::test]
    async fn test_recording_empty_sources_match_all() {
        let rec = Recording::new(1, String::new(), vec![], None, None);

        assert!(rec.matches_source("any"));
        assert!(rec.matches_source("dut1"));
    }

    #[tokio::test]
    async fn test_recording_evaluate_event() {
        let match_all = Some(Jmes::new("`true`").unwrap());
        let mut rec = Recording::new(1, String::new(), vec![], match_all, None);

        let event = json!({"type": "test", "value": 42});
        let finished = rec.evaluate_event("src", &event);

        assert!(!finished);
        assert_eq!(rec.events.len(), 1);
        assert_eq!(rec.events[0].payload, event);
        assert_eq!(rec.events[0].source, "src");
        assert!(rec.events[0].until_predicate.is_none());
    }

    #[tokio::test]
    async fn test_ingress_to_recording() {
        let state = Arc::new(AppState::new());

        // Start a recording for dut1 — matching="@" to record all
        let match_all = Some(Jmes::new("`true`").unwrap());
        let ref1 = state
            .start_recording(
                String::new(),
                vec!["dut1".to_string()],
                match_all,
                None,
            )
            .await;

        // Create mock ingress
        let ingress = Box::new(MockIngress::new(MockConfig {
            rate: 0,
            sources: vec![MockSource {
                name: "dut1".to_string(),
                pattern: MockPattern::Counter,
            }],
            limit: Some(5),
        }));

        // Process events
        let handle = spawn_ingress_processor(ingress, Arc::clone(&state));
        handle.await.unwrap();

        // Check recording received events
        let snap = state.get_recording(ref1).await.unwrap();
        assert_eq!(snap.events.len(), 5);
    }

    #[tokio::test]
    async fn test_multiple_recordings_same_source() {
        let state = Arc::new(AppState::new());

        // Start two recordings for dut1 — matching="@" to record all
        let match_all = || Some(Jmes::new("`true`").unwrap());
        let ref1 = state
            .start_recording(String::new(), vec!["dut1".to_string()], match_all(), None)
            .await;
        let ref2 = state
            .start_recording(String::new(), vec!["dut1".to_string()], match_all(), None)
            .await;

        // Create mock ingress
        let ingress = Box::new(MockIngress::new(MockConfig {
            rate: 0,
            sources: vec![MockSource {
                name: "dut1".to_string(),
                pattern: MockPattern::Counter,
            }],
            limit: Some(3),
        }));

        // Process events
        let handle = spawn_ingress_processor(ingress, Arc::clone(&state));
        handle.await.unwrap();

        // Both recordings should have received all events
        let snap1 = state.get_recording(ref1).await.unwrap();
        let snap2 = state.get_recording(ref2).await.unwrap();
        assert_eq!(snap1.events.len(), 3);
        assert_eq!(snap2.events.len(), 3);
    }

    // =========================================================================
    // Tests that were initially implemented outside of here
    // =========================================================================

    #[tokio::test]
    async fn test_recording_matching_predicate_filters() {

        let matching = Jmes::new("body.signal < `-65`").unwrap();
        let mut rec = Recording::new(1, String::new(), vec![], Some(matching), None);

        assert_eq!(rec.status, RecordingStatus::Running);

        // Event that matches (signal -67 < -65)
        let hit = json!({"event": "signal", "body": {"signal": -67}});
        rec.evaluate_event("s", &hit);
        assert_eq!(rec.events.len(), 1);

        // Another match
        rec.evaluate_event("s", &hit);
        assert_eq!(rec.events.len(), 2);

        // Event that does NOT match (signal -60 is not < -65)
        let miss = json!({"event": "signal", "body": {"signal": -60}});
        rec.evaluate_event("s", &miss);
        assert_eq!(rec.events.len(), 2); // unchanged
    }

    #[tokio::test]
    async fn test_recording_until_stops() {

        let matching = Jmes::new("body.signal < `-65`").unwrap();
        let until = JmesUntil::order(&["event == 'final'"]).unwrap();
        let mut rec = Recording::new(1, String::new(), vec![], Some(matching), Some(until));

        assert_eq!(rec.status, RecordingStatus::Running);

        // Matching event recorded
        let hit = json!({"event": "signal", "body": {"signal": -67}});
        assert!(!rec.evaluate_event("s", &hit));
        assert_eq!(rec.events.len(), 1);

        // Until event stops the recording
        let stop = json!({"event": "final", "body": {"value": 10}});
        assert!(rec.evaluate_event("s", &stop));
        assert_eq!(rec.status, RecordingStatus::Completed);
        assert_eq!(rec.events[1].until_predicate, Some(1));
    }

    #[tokio::test]
    async fn test_recording_no_until_stays_active() {
        let matching = Jmes::new("body.signal < `-65`").unwrap();
        let rec = Recording::new(1, String::new(), vec![], Some(matching), None);

        assert_eq!(rec.status, RecordingStatus::Running);
    }

    #[tokio::test]
    async fn test_stop_sets_finished_at_ms() {
        let state = Arc::new(AppState::new());
        let matching = Jmes::new("`true`").unwrap();
        let ref1 = state
            .start_recording(String::new(), vec!["s".to_string()], Some(matching), None)
            .await;

        let event = serde_json::json!({"x": 1});
        state.process_event(&Event { source: "s".to_string(), payload: event }).await;

        state.stop_recording(ref1).await;

        let snap = state.get_recording(ref1).await.unwrap();
        assert_eq!(snap.info.status, RecordingStatus::Stopped);
        assert!(snap.info.finished_at_ms.is_some(), "stop must set finished_at_ms");
        assert!(snap.info.finished_at_ms.unwrap() >= snap.info.last_event_at_ms.unwrap());
    }

    #[tokio::test]
    async fn test_stop_completed_recording_preserves_status() {
        let state = Arc::new(AppState::new());
        let until = JmesUntil::order(&["event == 'done'"]).unwrap();
        let ref1 = state
            .start_recording(String::new(), vec!["s".to_string()], None, Some(until))
            .await;

        // Complete the recording via until-predicate
        let event = Event { source: "s".to_string(), payload: json!({"event": "done"}) };
        state.process_event(&event).await;

        let snap = state.get_recording(ref1).await.unwrap();
        assert_eq!(snap.info.status, RecordingStatus::Completed);
        let original_finished_at = snap.info.finished_at_ms;

        // Stop should not clobber Completed status or timestamp
        state.stop_recording(ref1).await;

        let snap = state.get_recording(ref1).await.unwrap();
        assert_eq!(snap.info.status, RecordingStatus::Completed);
        assert_eq!(snap.info.finished_at_ms, original_finished_at);
    }

    #[tokio::test]
    async fn test_no_matching_records_only_until_events() {
        // No matching predicate → only until-trigger events are recorded
        let until = JmesUntil::order(&["event == 'start'", "event == 'done'"]).unwrap();
        let mut rec = Recording::new(1, String::new(), vec![], None, Some(until));

        // Non-until event: silently dropped (no matching predicate)
        rec.evaluate_event("s", &json!({"event": "noise", "val": 1}));
        assert_eq!(rec.events.len(), 0);

        // Until event: always recorded
        rec.evaluate_event("s", &json!({"event": "start"}));
        assert_eq!(rec.events.len(), 1);
        assert_eq!(rec.events[0].until_predicate, Some(1));

        // More noise: still dropped
        rec.evaluate_event("s", &json!({"event": "noise", "val": 2}));
        assert_eq!(rec.events.len(), 1);

        // Final until event: recorded, recording finishes
        rec.evaluate_event("s", &json!({"event": "done"}));
        assert_eq!(rec.events.len(), 2);
        assert_eq!(rec.status, RecordingStatus::Completed);
    }

    #[tokio::test]
    async fn test_no_matching_no_until_records_nothing() {
        // Permutation #1: no matching, no until → records nothing, never finishes
        let mut rec = Recording::new(1, String::new(), vec![], None, None);

        rec.evaluate_event("s", &json!({"event": "anything"}));
        rec.evaluate_event("s", &json!({"event": "something_else"}));
        assert_eq!(rec.events.len(), 0);
        assert_eq!(rec.status, RecordingStatus::Running);
    }

    #[tokio::test]
    async fn test_matching_with_until_event_matches_neither() {
        // Permutation #8: event matches neither matching nor until → dropped
        let matching = Jmes::new("type == 'wanted'").unwrap();
        let until = JmesUntil::order(&["event == 'done'"]).unwrap();
        let mut rec = Recording::new(1, String::new(), vec![], Some(matching), Some(until));

        rec.evaluate_event("s", &json!({"type": "unwanted", "event": "noise"}));
        assert_eq!(rec.events.len(), 0);
        assert_eq!(rec.status, RecordingStatus::Running);
    }

    #[tokio::test]
    async fn test_until_event_recorded_even_if_matching_would_reject() {
        // Until takes priority: event that matches until but NOT matching
        // is still recorded (until consumes before matching is checked)
        let matching = Jmes::new("type == 'wanted'").unwrap();
        let until = JmesUntil::order(&["event == 'done'"]).unwrap();
        let mut rec = Recording::new(1, String::new(), vec![], Some(matching), Some(until));

        // Event matches matching but not until → recorded by matching
        rec.evaluate_event("s", &json!({"type": "wanted", "event": "data"}));
        assert_eq!(rec.events.len(), 1);
        assert!(rec.events[0].until_predicate.is_none());

        // Event matches until but NOT matching → still recorded by until
        rec.evaluate_event("s", &json!({"type": "unwanted", "event": "done"}));
        assert_eq!(rec.events.len(), 2);
        assert_eq!(rec.events[1].until_predicate, Some(1));
        assert_eq!(rec.status, RecordingStatus::Completed);
    }

    #[tokio::test]
    async fn test_recordings_insert_and_list() {
        let mut recordings = Recordings::new();

        recordings.insert(Recording::new(1, String::new(), vec![], None, None));
        recordings.insert(Recording::new(2, String::new(), vec![], None, None));

        let list = recordings.list();
        assert_eq!(list.len(), 2);

        let refs: Vec<u64> = list.iter().map(|r| r.reference).collect();
        assert!(refs.contains(&1));
        assert!(refs.contains(&2));
    }

    #[tokio::test]
    async fn test_recordings_delete() {
        let mut recordings = Recordings::new();

        recordings.insert(Recording::new(1, String::new(), vec![], None, None));
        assert!(recordings.delete(1));
        assert!(recordings.get_events(1).is_none());

        // Deleting non-existent returns false
        assert!(!recordings.delete(999));
    }

    #[tokio::test]
    async fn test_recordings_fanout_with_different_predicates() {
        // Three recordings with different matching predicates receive the same events.
        let mut recordings = Recordings::new();

        let pred1 = Jmes::new("body.signal == `-55`").unwrap();
        let pred2 = Jmes::new("body.signal < `-60`").unwrap();
        let pred3 = Jmes::new("body.mode == '5G NR'").unwrap();

        recordings.insert(Recording::new(1, String::new(), vec![], Some(pred1), None));
        recordings.insert(Recording::new(2, String::new(), vec![], Some(pred2), None));
        recordings.insert(Recording::new(3, String::new(), vec![], Some(pred3), None));
        let (r1, r2, r3) = (1, 2, 3);

        let events = vec![
            json!({"event": "signal", "body": {"signal": -55, "mode": "5G NR"}}),
            json!({"event": "signal", "body": {"signal": -61, "mode": "5G NR"}}),
            json!({"event": "signal", "body": {"signal": -70, "mode": "5G NR"}}),
        ];

        // Fan out each event to all active recordings
        for event in &events {
            for rec in recordings.active_for_source("src") {
                rec.evaluate_event("src", event);
            }
        }

        // r1: only signal==-55 matches
        let e1 = recordings.get_events(r1).unwrap();
        assert_eq!(e1.len(), 1);
        assert_eq!(e1[0].payload["body"]["signal"], -55);

        // r2: signal -61 and -70 are < -60
        let e2 = recordings.get_events(r2).unwrap();
        assert_eq!(e2.len(), 2);
        assert_eq!(e2[0].payload["body"]["signal"], -61);
        assert_eq!(e2[1].payload["body"]["signal"], -70);

        // r3: all three have mode == "5G NR"
        let e3 = recordings.get_events(r3).unwrap();
        assert_eq!(e3.len(), 3);
    }

    // =========================================================================
    // Source stats tracking
    // =========================================================================

    #[tokio::test]
    async fn test_source_stats_empty_initially() {
        let state = AppState::new();
        let stats = state.source_stats().await;
        assert!(stats.is_empty());
    }

    #[tokio::test]
    async fn test_source_stats_updated_on_process_event() {
        let state = AppState::new();

        for _ in 0..3 {
            state.process_event(&Event {
                source: "src1".into(),
                payload: json!({"v": 1}),
            }).await;
        }

        let stats = state.source_stats().await;
        assert_eq!(stats.len(), 1);
        let s = &stats[0];
        assert_eq!(s.source, "src1");
        assert_eq!(s.event_count, 3);
        assert!(s.last_seen_ms > 0);
    }

    #[tokio::test]
    async fn test_source_stats_multiple_sources() {
        let state = AppState::new();

        state.process_event(&Event {
            source: "s1".into(), payload: json!({}),
        }).await;
        state.process_event(&Event {
            source: "s2".into(), payload: json!({}),
        }).await;
        state.process_event(&Event {
            source: "s1".into(), payload: json!({}),
        }).await;

        let stats = state.source_stats().await;
        assert_eq!(stats.len(), 2);
        let s1 = stats.iter().find(|s| s.source == "s1").unwrap();
        let s2 = stats.iter().find(|s| s.source == "s2").unwrap();
        assert_eq!(s1.event_count, 2);
        assert_eq!(s2.event_count, 1);
    }

    // =========================================================================
    // Recording accessors: matching_expr, until_progress
    // =========================================================================

    #[tokio::test]
    async fn test_recording_matching_expr_some_and_none() {
        let rec_with = Recording::new(
            1, String::new(), vec![],
            Some(Jmes::new("type == 'test'").unwrap()),
            None,
        );
        assert_eq!(rec_with.matching_expr(), Some("type == 'test'"));

        let rec_without = Recording::new(1, String::new(), vec![], None, None);
        assert_eq!(rec_without.matching_expr(), None);
    }

    #[tokio::test]
    async fn test_until_progress_none_without_until() {
        let rec = Recording::new(1, String::new(), vec![], None, None);
        assert!(rec.until_progress().is_none());
    }

    #[tokio::test]
    async fn test_until_progress_order_tracks_matched() {
        let until = JmesUntil::order(&[
            "event == 'a'", "event == 'b'", "event == 'c'",
        ]).unwrap();
        let mut rec = Recording::new(1, String::new(), vec![], None, Some(until));

        // Before any matching
        let p = rec.until_progress().unwrap();
        assert_eq!(p.r#type, "order");
        assert_eq!(p.predicates.len(), 3);
        assert!(!p.predicates[0].matched);
        assert_eq!(p.predicates[0].expr, "event == 'a'");
        assert_eq!(p.predicates[0].index, 1);

        // Match first predicate
        rec.evaluate_event("s", &json!({"event": "a"}));
        let p = rec.until_progress().unwrap();
        assert!(p.predicates[0].matched);
        assert!(!p.predicates[1].matched);
        assert!(!p.predicates[2].matched);
    }

    #[tokio::test]
    async fn test_until_progress_any_order() {
        let until = JmesUntil::any_order(&["x == `1`", "y == `2`"]).unwrap();
        let rec = Recording::new(1, String::new(), vec![], None, Some(until));

        let p = rec.until_progress().unwrap();
        assert_eq!(p.r#type, "any_order");
        assert_eq!(p.predicates.len(), 2);
    }

    #[tokio::test]
    async fn test_recording_info_has_matching_and_until() {
        let state = AppState::new();
        let matching = Jmes::new("type == 'temp'").unwrap();
        let until = JmesUntil::order(&["event == 'done'"]).unwrap();
        state.start_recording(
            String::new(), vec!["s".into()], Some(matching), Some(until),
        ).await;

        let list = state.list_recordings().await;
        assert_eq!(list.len(), 1);
        let info = &list[0];
        assert_eq!(info.matching_expr.as_deref(), Some("type == 'temp'"));
        let until = info.until.as_ref().unwrap();
        assert_eq!(until.r#type, "order");
        assert_eq!(until.predicates.len(), 1);
        assert_eq!(until.predicates[0].expr, "event == 'done'");
        assert!(!until.predicates[0].matched);
    }

    #[tokio::test]
    async fn test_recording_timestamps_and_counters() {
        let matching = Jmes::new("`true`").unwrap();
        let until = JmesUntil::order(&["event == 'done'"]).unwrap();
        let mut rec = Recording::new(
            1, String::new(), vec![], Some(matching), Some(until),
        );

        // Before any events
        assert!(rec.created_at_ms > 0);
        assert_eq!(rec.first_event_at_ms, None);
        assert_eq!(rec.last_event_at_ms, None);
        assert_eq!(rec.finished_at_ms, None);
        assert_eq!(rec.events_evaluated, 0);

        // After a non-finishing event
        let payload1 = serde_json::json!({"event": "noise"});
        rec.evaluate_event("s", &payload1);
        assert_eq!(rec.events_evaluated, 1);
        assert!(rec.first_event_at_ms.is_some());
        assert!(rec.last_event_at_ms.is_some());
        assert_eq!(rec.finished_at_ms, None);
        assert_eq!(rec.events.len(), 1);
        assert!(rec.events[0].recorded_at_ms > 0);

        let first_ts = rec.first_event_at_ms.unwrap();

        // After the finishing event
        let payload2 = serde_json::json!({"event": "done"});
        rec.evaluate_event("s", &payload2);
        assert_eq!(rec.events_evaluated, 2);
        assert_eq!(rec.first_event_at_ms.unwrap(), first_ts); // unchanged
        assert!(rec.last_event_at_ms.unwrap() >= first_ts);
        assert_eq!(rec.status, RecordingStatus::Completed);
        assert!(rec.finished_at_ms.is_some());
        assert!(rec.finished_at_ms.unwrap() >= rec.last_event_at_ms.unwrap());
    }

    #[tokio::test]
    async fn test_events_evaluated_counts_non_matching() {
        // No matching, no until — evaluate_event should still increment counter
        let mut rec = Recording::new(1, String::new(), vec![], None, None);

        let payload = serde_json::json!({"x": 1});
        rec.evaluate_event("s", &payload);
        rec.evaluate_event("s", &payload);
        rec.evaluate_event("s", &payload);

        assert_eq!(rec.events_evaluated, 3);
        assert_eq!(rec.events.len(), 0); // nothing recorded
        assert_eq!(rec.first_event_at_ms, None); // no events recorded
    }

    #[tokio::test]
    async fn test_info_includes_timestamps() {
        let matching = Jmes::new("`true`").unwrap();
        let mut rec = Recording::new(1, String::new(), vec![], Some(matching), None);

        let info_before = rec.info();
        assert!(info_before.created_at_ms > 0);
        assert_eq!(info_before.first_event_at_ms, None);
        assert_eq!(info_before.events_evaluated, 0);

        rec.evaluate_event("s", &serde_json::json!({"x": 1}));

        let info_after = rec.info();
        assert!(info_after.first_event_at_ms.is_some());
        assert!(info_after.last_event_at_ms.is_some());
        assert_eq!(info_after.events_evaluated, 1);
        assert_eq!(info_after.event_count, 1);
    }

    // =========================================================================
    // Reaper tests
    // =========================================================================

    #[tokio::test]
    async fn test_reap_deletes_finished_recording() {
        let state = AppState::new().with_ttl_secs(60);
        let matching = Jmes::new("`true`").unwrap();
        let ref1 = state
            .start_recording(String::new(), vec!["s".into()], Some(matching), None)
            .await;

        // Stop the recording
        state.stop_recording(ref1).await;
        let snap = state.get_recording(ref1).await.unwrap();
        let finished_at = snap.info.finished_at_ms.unwrap();

        // Reap at finished_at + 59s — should survive
        reap_expired(&state, finished_at + 59_000).await;
        assert!(state.get_recording(ref1).await.is_some());

        // Reap at finished_at + 60s — should be deleted
        reap_expired(&state, finished_at + 60_000).await;
        assert!(state.get_recording(ref1).await.is_none());
    }

    #[tokio::test]
    async fn test_reap_skips_running_recording_with_recent_access() {
        let state = AppState::new().with_ttl_secs(60);
        let matching = Jmes::new("`true`").unwrap();
        let ref1 = state
            .start_recording(String::new(), vec!["s".into()], Some(matching), None)
            .await;

        // Access it (get_recording touches last_accessed_at)
        let snap = state.get_recording(ref1).await.unwrap();
        assert_eq!(snap.info.status, RecordingStatus::Running);

        // Reap 30s after creation — well within TTL
        reap_expired(&state, now_ms() + 30_000).await;
        assert!(state.get_recording(ref1).await.is_some());
    }

    #[tokio::test]
    async fn test_reap_deletes_idle_running_recording() {
        let state = AppState::new().with_ttl_secs(60);
        let matching = Jmes::new("`true`").unwrap();
        let ref1 = state
            .start_recording(String::new(), vec!["s".into()], Some(matching), None)
            .await;

        let created = state.get_recording(ref1).await.unwrap().info.created_at_ms;

        // Reap 60s after last access (creation) — should be deleted
        reap_expired(&state, created + 60_000).await;
        assert!(state.get_recording(ref1).await.is_none());
    }

    #[tokio::test]
    async fn test_reap_spares_accessed_running_recording() {
        let state = AppState::new().with_ttl_secs(60);
        let matching = Jmes::new("`true`").unwrap();
        let ref1 = state
            .start_recording(String::new(), vec!["s".into()], Some(matching), None)
            .await;

        let created = state.get_recording(ref1).await.unwrap().info.created_at_ms;

        // Simulate access at created + 50s by touching directly
        {
            let shard_locks = state.find_recording_shards(ref1).await.unwrap();
            let shard = shard_locks[0].read().await;
            let rec = shard.get(ref1).unwrap();
            rec.last_accessed_at.store(created + 50_000, Ordering::Relaxed);
        }

        // Reap at created + 60s — recording was accessed at +50s, so only 10s idle
        reap_expired(&state, created + 60_000).await;
        assert!(state.get_recording(ref1).await.is_some());

        // Reap at created + 110s — 60s since last access, should be deleted
        reap_expired(&state, created + 110_000).await;
        assert!(state.get_recording(ref1).await.is_none());
    }

    #[tokio::test]
    async fn test_reap_prunes_empty_shards() {
        let state = AppState::new().with_ttl_secs(10);
        let matching = Jmes::new("`true`").unwrap();
        let ref1 = state
            .start_recording(String::new(), vec!["prunable".into()], Some(matching), None)
            .await;

        // Verify shard exists
        {
            let shards = state.shards.read().await;
            assert!(shards.contains_key("prunable"));
        }

        // Stop and reap
        state.stop_recording(ref1).await;
        let snap = state.get_recording(ref1).await.unwrap();
        reap_expired(&state, snap.info.finished_at_ms.unwrap() + 10_000).await;

        // Shard should be pruned
        {
            let shards = state.shards.read().await;
            assert!(!shards.contains_key("prunable"));
        }
    }
}
