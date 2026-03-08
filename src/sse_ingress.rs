//! SSE (Server-Sent Events) ingress
//!
//! Connects to an HTTP SSE endpoint and converts events to jmestrap Events.
//! Enabled with `--features sse`.
//!
//! # Example
//!
//! ```ignore
//! let config = SseIngressConfig {
//!     url: "http://example.com/stream_event".to_string(),
//!     source_field: "sourceId".to_string(),
//!     default_source: "unknown".to_string(),
//! };
//!
//! let ingress = SseIngress::connect(config).await?;
//! // Use with spawn_ingress_processor()
//! ```

use crate::ingress::{Event, EventIngress};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for SSE ingress
#[derive(Debug, Clone, Deserialize)]
pub struct SseIngressConfig {
    /// SSE endpoint URL (e.g., "http://host:5000/stream_event")
    pub url: String,

    /// Dotted JSON field path for source extraction (e.g., "sourceId")
    pub source_field: String,

    /// Fallback source when field is missing
    #[serde(default = "default_source")]
    pub default_source: String,
}

fn default_source() -> String {
    "unknown".to_string()
}

impl SseIngressConfig {
    /// Simple config: source from `sourceId` field
    #[allow(dead_code)]
    pub fn simple(url: &str) -> Self {
        Self {
            url: url.to_string(),
            source_field: "sourceId".to_string(),
            default_source: "unknown".to_string(),
        }
    }

    /// Config matching a common bridge SSE format
    #[allow(dead_code)]
    pub fn bridge_profile(url: &str) -> Self {
        Self {
            url: url.to_string(),
            source_field: "sourceId".to_string(),
            default_source: "unknown".to_string(),
        }
    }
}

// =============================================================================
// JSON field extraction
// =============================================================================

/// Extract a string value from a JSON object using a dotted path.
///
/// `"sourceId"` → `payload["sourceId"]`
/// `"properties.event"` → `payload["properties"]["event"]`
fn extract_field<'a>(value: &'a JsonValue, dotted_path: &str) -> Option<&'a str> {
    let mut current = value;
    for key in dotted_path.split('.') {
        current = current.get(key)?;
    }
    current.as_str()
}

// =============================================================================
// SseIngress
// =============================================================================

/// SSE ingress — connects to an HTTP SSE endpoint and produces Events.
pub struct SseIngress {
    rx: mpsc::Receiver<Event>,
    _handle: tokio::task::JoinHandle<()>,
}

impl SseIngress {
    /// Connect to an SSE endpoint and start receiving events.
    pub async fn connect(config: SseIngressConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // Verify the endpoint is reachable with a HEAD request
        let client = reqwest::Client::new();
        let resp = client
            .get(&config.url)
            .header("Accept", "text/event-stream")
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(format!(
                "SSE endpoint returned {}: {}",
                resp.status(),
                config.url
            )
            .into());
        }

        let (tx, rx) = mpsc::channel(1024);

        let handle = tokio::spawn(Self::reader_task(config.clone(), resp, tx.clone()));

        eprintln!(
            "[sse] Connected to {}, source_field={}",
            config.url,
            config.source_field,
        );

        Ok(Self {
            rx,
            _handle: handle,
        })
    }

    /// Background task: read SSE stream, parse lines, send Events.
    async fn reader_task(
        config: SseIngressConfig,
        mut response: reqwest::Response,
        tx: mpsc::Sender<Event>,
    ) {
        let mut buffer = String::new();

        while let Ok(Some(chunk)) = response.chunk().await {
            let text = match std::str::from_utf8(&chunk) {
                Ok(t) => t,
                Err(_) => continue,
            };

            buffer.push_str(text);

            // Process complete lines from the buffer.
            // Keep at most one trailing partial line between chunks.
            let mut consumed = 0usize;
            while let Some(rel_newline) = buffer[consumed..].find('\n') {
                let newline_pos = consumed + rel_newline;
                let line = buffer[consumed..newline_pos].trim_end_matches('\r');

                if let Some(event) = parse_sse_data_line(line, &config) {
                    if tx.send(event).await.is_err() {
                        return; // receiver dropped
                    }
                }

                consumed = newline_pos + 1;
            }

            if consumed > 0 {
                buffer.drain(..consumed);
            }
        }

        eprintln!("[sse] Stream ended: {}", config.url);
    }
}

fn parse_sse_data_line(line: &str, config: &SseIngressConfig) -> Option<Event> {
    if line.is_empty() {
        // Empty line = end of SSE event frame
        return None;
    }
    if line.starts_with(':') {
        // Comment / keepalive (": ping")
        return None;
    }

    let data = line.strip_prefix("data:")?;
    let json_str = data.trim_start();
    if json_str.is_empty() {
        return None;
    }

    match serde_json::from_str::<JsonValue>(json_str) {
        Ok(payload) => {
            let source = extract_field(&payload, &config.source_field)
                .unwrap_or(&config.default_source)
                .to_string();

            Some(Event {
                source,
                payload,
            })
        }
        Err(e) => {
            eprintln!("[sse] JSON parse error: {}", e);
            None
        }
    }
}

impl EventIngress for SseIngress {
    fn recv(&mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<Event>> + Send + '_>> {
        Box::pin(self.rx.recv())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_field_simple() {
        let v = serde_json::json!({"sourceId": "node_4", "seq": 1});
        assert_eq!(extract_field(&v, "sourceId"), Some("node_4"));
    }

    #[test]
    fn test_extract_field_dotted() {
        let v = serde_json::json!({
            "properties": {"event": "ttime", "id": "123"},
            "body": {"ts": 100}
        });
        assert_eq!(extract_field(&v, "properties.event"), Some("ttime"));
    }

    #[test]
    fn test_extract_field_missing() {
        let v = serde_json::json!({"sourceId": "node_4"});
        assert_eq!(extract_field(&v, "missing"), None);
        assert_eq!(extract_field(&v, "a.b.c"), None);
    }

    #[test]
    fn test_extract_field_non_string() {
        let v = serde_json::json!({"count": 42});
        assert_eq!(extract_field(&v, "count"), None); // not a string
    }

    #[test]
    fn test_config_simple() {
        let config = SseIngressConfig::simple("http://localhost:5000/stream_event");
        assert_eq!(config.url, "http://localhost:5000/stream_event");
        assert_eq!(config.source_field, "sourceId");
    }

    #[test]
    fn test_config_deserialize() {
        let json = r#"{
            "url": "http://10.0.0.1:5000/stream_event",
            "source_field": "sourceId"
        }"#;
        let config: SseIngressConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.url, "http://10.0.0.1:5000/stream_event");
        assert_eq!(config.source_field, "sourceId");
        assert_eq!(config.default_source, "unknown");
    }

    // =========================================================================
    // Integration tests — mock SSE server
    // =========================================================================

    /// Spin up a minimal HTTP server that serves an SSE stream.
    /// Returns (url, shutdown_sender).
    async fn mock_sse_server(events: Vec<String>) -> (String, tokio::sync::oneshot::Sender<()>) {
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            // Accept one connection (the SseIngress client)
            let accept = tokio::select! {
                result = listener.accept() => Some(result),
                _ = &mut shutdown_rx => None,
            };
            let (mut stream, _) = match accept {
                Some(Ok(v)) => v,
                _ => return,
            };

            // Read the HTTP request (consume it, don't care about contents)
            let mut buf = [0u8; 4096];
            let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;

            // Send HTTP response headers
            let headers = "HTTP/1.1 200 OK\r\n\
                           Content-Type: text/event-stream\r\n\
                           Cache-Control: no-cache\r\n\
                           Connection: keep-alive\r\n\
                           \r\n";
            let _ = stream.write_all(headers.as_bytes()).await;

            // Send SSE events
            for event_line in &events {
                let _ = stream.write_all(event_line.as_bytes()).await;
                let _ = stream.flush().await;
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }

            // Close connection (SseIngress will see stream end)
            drop(stream);
        });

        let url = format!("http://127.0.0.1:{}/stream_event", port);
        (url, shutdown_tx)
    }

    #[tokio::test]
    async fn test_sse_ingress_receives_events() {
        let events = vec![
            ": ping\n\n".to_string(),
            "data:{\"sourceId\":\"node_4\",\"properties\":{\"event\":\"temp\"},\"body\":{\"val\":22}}\n\n".to_string(),
            "data:{\"sourceId\":\"node_4\",\"properties\":{\"event\":\"pres\"},\"body\":{\"val\":97}}\n\n".to_string(),
            "data:{\"sourceId\":\"nuc6\",\"properties\":{\"event\":\"can\"},\"body\":{\"id\":42}}\n\n".to_string(),
        ];

        let (url, _shutdown) = mock_sse_server(events).await;

        let config = SseIngressConfig {
            url,
            source_field: "sourceId".to_string(),
            default_source: "unknown".to_string(),
        };

        let mut ingress = SseIngress::connect(config).await.unwrap();

        // Event 1: node_4
        let e1 = ingress.recv().await.unwrap();
        assert_eq!(e1.source, "node_4");
        assert_eq!(e1.payload["body"]["val"], 22);

        // Event 2: node_4
        let e2 = ingress.recv().await.unwrap();
        assert_eq!(e2.source, "node_4");

        // Event 3: nuc6
        let e3 = ingress.recv().await.unwrap();
        assert_eq!(e3.source, "nuc6");

        // Stream ended — recv returns None
        let end = ingress.recv().await;
        assert!(end.is_none());
    }

    #[tokio::test]
    async fn test_sse_ingress_missing_source_field_uses_default() {
        let events = vec![
            // No sourceId field — should fall back to default_source
            "data:{\"name\":\"sensor1\",\"value\":42}\n\n".to_string(),
        ];

        let (url, _shutdown) = mock_sse_server(events).await;

        let config = SseIngressConfig::simple(&url);
        let mut ingress = SseIngress::connect(config).await.unwrap();

        let e = ingress.recv().await.unwrap();
        assert_eq!(e.source, "unknown"); // default
    }

    #[tokio::test]
    async fn test_sse_ingress_skips_malformed_json() {
        let events = vec![
            "data:not json at all\n\n".to_string(),
            "data:{\"sourceId\":\"node_4\",\"body\":{\"val\":1}}\n\n".to_string(),
        ];

        let (url, _shutdown) = mock_sse_server(events).await;

        let config = SseIngressConfig::simple(&url);
        let mut ingress = SseIngress::connect(config).await.unwrap();

        // Should skip the malformed line and deliver the valid event
        let e = ingress.recv().await.unwrap();
        assert_eq!(e.source, "node_4");
    }

    #[tokio::test]
    async fn test_sse_ingress_malformed_burst_keeps_valid_events() {
        let events = vec![
            ": ping\n\n".to_string(),
            "data:not-json-1\n\n".to_string(),
            "data:not-json-2\n\n".to_string(),
            "event: telemetry\n".to_string(),
            "id: 101\n".to_string(),
            "data:{\"sourceId\":\"node_4\",\"body\":{\"val\":10}}\n\n".to_string(),
            "retry: 1000\n\n".to_string(),
            "data:not-json-3\n\n".to_string(),
            "data:{\"sourceId\":\"node_4\",\"body\":{\"val\":11}}\n\n".to_string(),
        ];

        let (url, _shutdown) = mock_sse_server(events).await;
        let config = SseIngressConfig::simple(&url);
        let mut ingress = SseIngress::connect(config).await.unwrap();

        let e1 = ingress.recv().await.unwrap();
        assert_eq!(e1.source, "node_4");
        assert_eq!(e1.payload["body"]["val"], 10);

        let e2 = ingress.recv().await.unwrap();
        assert_eq!(e2.source, "node_4");
        assert_eq!(e2.payload["body"]["val"], 11);

        let end = ingress.recv().await;
        assert!(end.is_none());
    }

    #[tokio::test]
    async fn test_sse_ingress_disconnect_mid_event_drops_partial_line() {
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();

            let mut buf = [0u8; 4096];
            let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;

            let headers = "HTTP/1.1 200 OK\r\n\
                           Content-Type: text/event-stream\r\n\
                           Cache-Control: no-cache\r\n\
                           Connection: keep-alive\r\n\
                           \r\n";
            let _ = stream.write_all(headers.as_bytes()).await;

            // Valid event first.
            let _ = stream
                .write_all(b"data:{\"sourceId\":\"node_4\",\"body\":{\"val\":1}}\n\n")
                .await;
            let _ = stream.flush().await;

            // Partial event line without newline, then disconnect.
            let _ = stream
                .write_all(b"data:{\"sourceId\":\"node_4\",\"body\":{\"val\":2}")
                .await;
            let _ = stream.flush().await;
            drop(stream);
        });

        let url = format!("http://127.0.0.1:{}/stream_event", port);
        let config = SseIngressConfig::simple(&url);
        let mut ingress = SseIngress::connect(config).await.unwrap();

        let first = ingress.recv().await.unwrap();
        assert_eq!(first.payload["body"]["val"], 1);

        // Stream ended after a partial line; no second event should appear.
        let end = ingress.recv().await;
        assert!(end.is_none());
    }

    #[tokio::test]
    async fn test_sse_ingress_with_recordings() {
        use crate::core::{spawn_ingress_processor, AppState};
        use crate::control::{RecordingControl, StartRequest, GetEventsRequest};
        use std::sync::Arc;

        let events = vec![
            "data:{\"sourceId\":\"node_4\",\"properties\":{\"event\":\"temp\"},\"body\":{\"val\":22}}\n\n".to_string(),
            "data:{\"sourceId\":\"node_4\",\"properties\":{\"event\":\"done\"},\"body\":{}}\n\n".to_string(),
        ];

        let (url, _shutdown) = mock_sse_server(events).await;

        let config = SseIngressConfig::bridge_profile(&url);
        let ingress = SseIngress::connect(config).await.unwrap();

        // Wire into AppState + Recording
        let state = Arc::new(AppState::new());

        let ref_id = state.start(StartRequest {
            description: String::new(),
            sources: vec!["node_4".into()],
            matching: Some("@".into()),
            until: Some(crate::predicates::UntilSpec::Order {
                predicates: vec!["properties.event == 'done'".into()],
            }),
        }).await.unwrap().reference;

        spawn_ingress_processor(Box::new(ingress), Arc::clone(&state));

        // Long-poll: wait for recording to finish
        let resp = state.get_events(GetEventsRequest {
            reference: ref_id,
            timeout: Some(5.0),
        }).await.unwrap();

        assert!(resp.finished);
        assert_eq!(resp.events.len(), 2);
        assert_eq!(resp.events[0].payload["body"]["val"], 22);
    }
}
