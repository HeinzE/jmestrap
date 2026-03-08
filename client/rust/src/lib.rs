//! jmestrap-client — Rust client library for the JMESTrap REST API.
//!
//! # Quick start
//!
//! ```no_run
//! use jmestrap_client::{JmesTrap, Until};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), jmestrap_client::Error> {
//!     let trap = JmesTrap::new("http://127.0.0.1:9000");
//!
//!     let rec = trap.record()
//!         .sources(&["sensor_1"])
//!         .until(Until::order(["event == 'start'", "event == 'done'"]))
//!         .start()
//!         .await?;
//!
//!     // ... events arrive from MQTT / SSE / test harness ...
//!
//!     let result = rec.fetch(15.0).await?;
//!     assert!(result.finished);
//!     println!("{result}");
//!
//!     rec.delete().await?;
//!     Ok(())
//! }
//! ```
//!
//! # Test harness pattern
//!
//! ```no_run
//! use jmestrap_client::{JmesTrap, Until};
//!
//! #[tokio::test]
//! async fn test_login_logout() {
//!     let trap = JmesTrap::new("http://127.0.0.1:9000");
//!
//!     let rec = trap.record()
//!         .sources(&["keyboard_1"])
//!         .until(Until::order(["event == 'login'", "event == 'logout'"]))
//!         .start()
//!         .await
//!         .unwrap();
//!
//!     // ... trigger the device under test ...
//!
//!     let result = rec.fetch(15.0).await.unwrap();
//!     result.assert_finished("login sequence incomplete");
//! }
//! ```

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::fmt;
use std::time::Duration;

// =============================================================================
// Error
// =============================================================================

/// Client error type.
#[derive(Debug)]
pub enum Error {
    /// HTTP or network error.
    Http(reqwest::Error),
    /// Server returned a non-success status.
    Server { status: u16, message: String },
    /// Invalid predicate expression (422).
    InvalidPredicate(String),
    /// Recording not found (404).
    NotFound(u64),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Http(e) => write!(f, "HTTP error: {e}"),
            Error::Server { status, message } => write!(f, "server error {status}: {message}"),
            Error::InvalidPredicate(msg) => write!(f, "invalid predicate: {msg}"),
            Error::NotFound(r) => write!(f, "recording {r} not found"),
        }
    }
}

impl std::error::Error for Error {}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::Http(e)
    }
}

// =============================================================================
// Until — completion condition
// =============================================================================

/// Recording completion condition.
///
/// Determines when a recording finishes based on JMESPath predicates
/// evaluated against incoming event payloads.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Until {
    /// Predicates must match in sequence.
    Order { predicates: Vec<String> },
    /// All predicates must match, in any order.
    AnyOrder { predicates: Vec<String> },
}

impl Until {
    /// Ordered sequence: recording finishes when predicates match in order.
    ///
    /// ```
    /// use jmestrap_client::Until;
    /// let u = Until::order(["event == 'start'", "event == 'done'"]);
    /// ```
    pub fn order<I, S>(predicates: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Until::Order {
            predicates: predicates.into_iter().map(Into::into).collect(),
        }
    }

    /// Unordered: recording finishes when all predicates have matched.
    ///
    /// ```
    /// use jmestrap_client::Until;
    /// let u = Until::any_order(["temp > `30`", "humidity < `20`"]);
    /// ```
    pub fn any_order<I, S>(predicates: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Until::AnyOrder {
            predicates: predicates.into_iter().map(Into::into).collect(),
        }
    }
}

// =============================================================================
// Response types
// =============================================================================

#[derive(Debug, Deserialize)]
struct StartResponse {
    reference: u64,
}

/// A recorded event with envelope metadata.
#[derive(Debug, Deserialize)]
pub struct RecordedEvent {
    pub source: String,
    pub payload: JsonValue,
    #[serde(default)]
    pub until_predicate: Option<usize>,
    #[serde(default)]
    pub recorded_at_ms: u64,
}

/// Result of fetching a recording.
#[derive(Debug, Deserialize)]
pub struct RecordingResult {
    pub reference: u64,
    #[serde(default)]
    pub description: String,
    pub sources: Vec<String>,
    pub finished: bool,
    pub active: bool,
    pub event_count: usize,
    pub events: Vec<RecordedEvent>,
    #[serde(default)]
    pub matching_expr: Option<String>,
    #[serde(default)]
    pub until: Option<UntilProgress>,
    #[serde(default)]
    pub created_at_ms: u64,
    #[serde(default)]
    pub first_event_at_ms: Option<u64>,
    #[serde(default)]
    pub last_event_at_ms: Option<u64>,
    #[serde(default)]
    pub finished_at_ms: Option<u64>,
    #[serde(default)]
    pub events_evaluated: u64,
}

/// Progress of an `until` completion condition.
#[derive(Debug, Deserialize)]
pub struct UntilProgress {
    /// `"order"` or `"any_order"`.
    #[serde(rename = "type")]
    pub kind: String,
    /// Per-predicate match status.
    pub predicates: Vec<PredicateProgress>,
}

/// Status of a single predicate within an `until` condition.
#[derive(Debug, Deserialize)]
pub struct PredicateProgress {
    /// 1-based predicate index.
    pub index: usize,
    /// The JMESPath expression.
    pub expr: String,
    /// Whether this predicate has matched.
    pub matched: bool,
}

/// Summary of a recording (from list endpoint).
#[derive(Debug, Deserialize)]
pub struct RecordingInfo {
    pub reference: u64,
    #[serde(default)]
    pub description: String,
    pub sources: Vec<String>,
    pub event_count: usize,
    pub active: bool,
    pub finished: bool,
    #[serde(default)]
    pub matching_expr: Option<String>,
    #[serde(default)]
    pub until: Option<UntilProgress>,
    #[serde(default)]
    pub created_at_ms: u64,
    #[serde(default)]
    pub first_event_at_ms: Option<u64>,
    #[serde(default)]
    pub last_event_at_ms: Option<u64>,
    #[serde(default)]
    pub finished_at_ms: Option<u64>,
    #[serde(default)]
    pub events_evaluated: u64,
}

#[derive(Debug, Deserialize)]
struct ListResponse {
    recordings: Vec<RecordingInfo>,
}

/// Observed event source with statistics.
#[derive(Debug, Deserialize)]
pub struct SourceInfo {
    pub source: String,
    pub event_count: u64,
    pub last_seen_ms: u64,
    #[serde(default)]
    pub active_recording_count: usize,
}

#[derive(Debug, Deserialize)]
struct SourcesResponse {
    sources: Vec<SourceInfo>,
}

// =============================================================================
// RecordingResult — display and assertions
// =============================================================================

impl RecordingResult {
    /// Panics with a predicate report if the recording did not finish.
    ///
    /// Intended for `#[tokio::test]` assertions:
    ///
    /// ```no_run
    /// # let result: jmestrap_client::RecordingResult = todo!();
    /// result.assert_finished("login sequence incomplete");
    /// ```
    pub fn assert_finished(&self, msg: &str) {
        if !self.finished {
            panic!("{msg}\n{self}");
        }
    }

    /// Iterator over predicates that have NOT matched.
    pub fn failed_predicates(&self) -> impl Iterator<Item = &PredicateProgress> {
        self.until
            .iter()
            .flat_map(|u| u.predicates.iter())
            .filter(|p| !p.matched)
    }
}

impl fmt::Display for RecordingResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = if self.finished { "finished" } else { "active" };
        writeln!(
            f,
            "Recording {} — {state} — {} events",
            self.reference,
            self.events.len()
        )?;
        if let Some(progress) = &self.until {
            for p in &progress.predicates {
                let mark = if p.matched { "✅" } else { "🔴" };
                writeln!(f, "  {mark} [{:>2}] {:?}", p.index, p.expr)?;
            }
        }
        Ok(())
    }
}

// =============================================================================
// Recording — live handle
// =============================================================================

/// Handle to a recording on the server.
///
/// Use [`fetch`](Recording::fetch) to long-poll for results.
/// The handle is cheap to hold — it does not keep a connection open.
pub struct Recording {
    /// Server-assigned reference.
    pub reference: u64,
    base: String,
    client: Client,
}

impl Recording {
    /// Long-poll for recording results.
    ///
    /// Blocks up to `timeout` seconds waiting for the recording to finish.
    /// Returns the current state whether finished or not.
    pub async fn fetch(&self, timeout: f64) -> Result<RecordingResult, Error> {
        let resp = self
            .client
            .get(format!(
                "{}/recordings/{}?timeout={timeout}",
                self.base, self.reference
            ))
            .timeout(Duration::from_secs_f64(timeout + 5.0))
            .send()
            .await?;

        match resp.status().as_u16() {
            200 => Ok(resp.json().await?),
            404 => Err(Error::NotFound(self.reference)),
            s => Err(Error::Server {
                status: s,
                message: resp.text().await.unwrap_or_default(),
            }),
        }
    }

    /// Stop the recording early (before until predicates complete).
    pub async fn stop(&self) -> Result<(), Error> {
        check(
            self.client
                .post(format!(
                    "{}/recordings/{}/stop",
                    self.base, self.reference
                ))
                .send()
                .await?,
        )
        .await
    }

    /// Delete this recording from the server.
    pub async fn delete(&self) -> Result<(), Error> {
        check(
            self.client
                .delete(format!("{}/recordings/{}", self.base, self.reference))
                .send()
                .await?,
        )
        .await
    }
}

impl fmt::Debug for Recording {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Recording")
            .field("reference", &self.reference)
            .finish()
    }
}

// =============================================================================
// RecordBuilder
// =============================================================================

/// Builder for creating a recording. Obtained from [`JmesTrap::record`].
pub struct RecordBuilder<'a> {
    trap: &'a JmesTrap,
    description: String,
    sources: Vec<String>,
    matching: Option<String>,
    until: Option<Until>,
}

impl<'a> RecordBuilder<'a> {
    /// Optional human-readable label for this recording.
    pub fn description(mut self, desc: &str) -> Self {
        self.description = desc.to_string();
        self
    }

    /// Sources to record from. Empty = all sources.
    pub fn sources(mut self, sources: &[&str]) -> Self {
        self.sources = sources.iter().map(|s| s.to_string()).collect();
        self
    }

    /// JMESPath filter — only events where this evaluates to true are recorded.
    pub fn matching(mut self, expr: &str) -> Self {
        self.matching = Some(expr.to_string());
        self
    }

    /// Completion condition.
    pub fn until(mut self, until: Until) -> Self {
        self.until = Some(until);
        self
    }

    /// Start the recording on the server. Returns a live handle.
    pub async fn start(self) -> Result<Recording, Error> {
        #[derive(Serialize)]
        struct Body {
            #[serde(skip_serializing_if = "String::is_empty")]
            description: String,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            sources: Vec<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            matching: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            until: Option<Until>,
        }

        let resp = self
            .trap
            .client
            .post(format!("{}/recordings", self.trap.base))
            .json(&Body {
                description: self.description,
                sources: self.sources,
                matching: self.matching,
                until: self.until,
            })
            .send()
            .await?;

        match resp.status().as_u16() {
            201 => {
                let r: StartResponse = resp.json().await?;
                Ok(Recording {
                    reference: r.reference,
                    base: self.trap.base.clone(),
                    client: self.trap.client.clone(),
                })
            }
            422 => {
                let body: JsonValue = resp.json().await?;
                Err(Error::InvalidPredicate(
                    body["error"].as_str().unwrap_or("unknown").to_string(),
                ))
            }
            s => Err(Error::Server {
                status: s,
                message: resp.text().await.unwrap_or_default(),
            }),
        }
    }
}

// =============================================================================
// JmesTrap — client
// =============================================================================

/// Client for the JMESTrap REST API.
///
/// Cheaply cloneable (wraps a shared `reqwest::Client`).
#[derive(Clone)]
pub struct JmesTrap {
    base: String,
    client: Client,
}

impl JmesTrap {
    /// Create a new client pointing at the given base URL.
    ///
    /// ```
    /// let trap = jmestrap_client::JmesTrap::new("http://127.0.0.1:9000");
    /// ```
    pub fn new(base_url: &str) -> Self {
        JmesTrap {
            base: base_url.trim_end_matches('/').to_string(),
            client: Client::new(),
        }
    }

    /// Health check. Returns `Ok(())` if the server is reachable.
    pub async fn ping(&self) -> Result<(), Error> {
        check(
            self.client
                .get(format!("{}/ping", self.base))
                .send()
                .await?,
        )
        .await
    }

    /// Start building a recording.
    ///
    /// ```no_run
    /// # let trap = jmestrap_client::JmesTrap::new("http://localhost:9000");
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let rec = trap.record()
    ///     .sources(&["sensor_1"])
    ///     .matching("type == 'protocol_frame'")
    ///     .until(jmestrap_client::Until::order(["state == 'init'", "state == 'ready'"]))
    ///     .start()
    ///     .await
    ///     .unwrap();
    /// # });
    /// ```
    pub fn record(&self) -> RecordBuilder<'_> {
        RecordBuilder {
            trap: self,
            description: String::new(),
            sources: vec![],
            matching: None,
            until: None,
        }
    }

    /// Inject an event directly (for testing and synthetic events).
    pub async fn inject(
        &self,
        source: &str,
        payload: &JsonValue,
    ) -> Result<(), Error> {
        check(
            self.client
                .post(format!("{}/events/{source}", self.base))
                .json(payload)
                .send()
                .await?,
        )
        .await
    }

    /// List all recordings on the server.
    pub async fn list_recordings(&self) -> Result<Vec<RecordingInfo>, Error> {
        let resp = self
            .client
            .get(format!("{}/recordings", self.base))
            .send()
            .await?;
        Ok(checked_json::<ListResponse>(resp).await?.recordings)
    }

    /// List observed event sources.
    pub async fn list_sources(&self) -> Result<Vec<SourceInfo>, Error> {
        let resp = self
            .client
            .get(format!("{}/sources", self.base))
            .send()
            .await?;
        Ok(checked_json::<SourcesResponse>(resp).await?.sources)
    }

    /// Delete all recordings. Useful for test teardown.
    pub async fn cleanup(&self) -> Result<(), Error> {
        for rec in self.list_recordings().await? {
            let _ = self
                .client
                .delete(format!("{}/recordings/{}", self.base, rec.reference))
                .send()
                .await;
        }
        Ok(())
    }
}

// =============================================================================
// Helpers
// =============================================================================

async fn check(resp: reqwest::Response) -> Result<(), Error> {
    let status = resp.status().as_u16();
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(Error::Server {
            status,
            message: resp.text().await.unwrap_or_default(),
        })
    }
}

async fn checked_json<T: serde::de::DeserializeOwned>(
    resp: reqwest::Response,
) -> Result<T, Error> {
    let status = resp.status().as_u16();
    if (200..300).contains(&status) {
        Ok(resp.json().await?)
    } else {
        Err(Error::Server {
            status,
            message: resp.text().await.unwrap_or_default(),
        })
    }
}
