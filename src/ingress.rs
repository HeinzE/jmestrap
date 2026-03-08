//! Event ingress abstraction and implementations
//!
//! EventIngress trait defines how events enter jmestrap.
//! Implementations: MockIngress (testing), SseIngress, MqttIngress.

#![allow(dead_code)] // Infrastructure for future transport implementations

use serde_json::Value as JsonValue;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;

// =============================================================================
// Core Types
// =============================================================================

/// A single inbound event from an event-source
#[derive(Debug, Clone)]
pub struct Event {
    /// Event-source identifier (e.g., "dut1")
    pub source: String,
    /// Event payload
    pub payload: JsonValue,
}

// =============================================================================
// EventIngress Trait
// =============================================================================

/// Trait for event ingress — implemented by transports.
///
/// Object-safe async trait: returns a pinned boxed future so that
/// ingress sources can be used as `Box<dyn EventIngress>`.
pub trait EventIngress: Send {
    /// Receive next event (blocks until available, None signals shutdown)
    fn recv(&mut self) -> Pin<Box<dyn Future<Output = Option<Event>> + Send + '_>>;
}

// =============================================================================
// MockIngress — Configurable event generator for testing
// =============================================================================

/// Configuration for mock event generation
#[derive(Debug, Clone)]
pub struct MockConfig {
    /// Events per second (0 = as fast as possible)
    pub rate: u32,
    /// Event-sources to simulate
    pub sources: Vec<MockSource>,
    /// Total events to generate (None = infinite)
    pub limit: Option<u64>,
}

/// A simulated event-source
#[derive(Debug, Clone)]
pub struct MockSource {
    pub name: String,
    /// Event pattern generator
    pub pattern: MockPattern,
}

/// Event payload patterns
#[derive(Debug, Clone)]
pub enum MockPattern {
    /// Sequential counter: {"seq": N, "type": "mock"}
    Counter,
    /// Protocol frames: {"type": "protocol", "id": N, "data": [...]}
    ProtocolFrame,
    /// State machine: {"state": "init"} -> {"state": "ready"} -> {"state": "done"}
    StateMachine { states: Vec<String> },
}

impl Default for MockConfig {
    fn default() -> Self {
        Self {
            rate: 100,
            sources: vec![MockSource {
                name: "mock1".to_string(),
                pattern: MockPattern::Counter,
            }],
            limit: None,
        }
    }
}

/// Mock ingress for testing — generates synthetic events
pub struct MockIngress {
    rx: mpsc::Receiver<Event>,
    // Handle kept to allow shutdown
    _handle: tokio::task::JoinHandle<()>,
}

impl MockIngress {
    /// Create a new mock ingress with the given configuration
    pub fn new(config: MockConfig) -> Self {
        let (tx, rx) = mpsc::channel(1024);
        
        let handle = tokio::spawn(async move {
            Self::generator(config, tx).await;
        });
        
        Self { rx, _handle: handle }
    }

    async fn generator(config: MockConfig, tx: mpsc::Sender<Event>) {
        let rate_interval = if config.rate > 0 {
            Some(interval(Duration::from_secs_f64(1.0 / config.rate as f64)))
        } else {
            None
        };

        let mut count: u64 = 0;
        let mut source_idx = 0;
        let mut state_idx: usize = 0;

        let mut ticker = rate_interval;

        loop {
            // Rate limiting
            if let Some(ref mut t) = ticker {
                t.tick().await;
            }

            // Check limit
            if let Some(limit) = config.limit {
                if count >= limit {
                    break;
                }
            }

            // Round-robin through sources
            if config.sources.is_empty() {
                break;
            }

            let source = &config.sources[source_idx % config.sources.len()];

            let payload = match &source.pattern {
                MockPattern::Counter => {
                    serde_json::json!({
                        "seq": count,
                        "type": "mock"
                    })
                }
                MockPattern::ProtocolFrame => {
                    serde_json::json!({
                        "type": "protocol",
                        "id": (count % 2048) as u32,
                        "data": [count as u8, (count >> 8) as u8, 0, 0, 0, 0, 0, 0]
                    })
                }
                MockPattern::StateMachine { states } => {
                    let state = &states[state_idx % states.len()];
                    state_idx += 1;
                    serde_json::json!({
                        "state": state,
                        "seq": count
                    })
                }
            };

            let event = Event {
                source: source.name.clone(),
                payload,
            };

            if tx.send(event).await.is_err() {
                // Receiver dropped, stop generating
                break;
            }

            count += 1;
            source_idx += 1;
        }
    }
}

impl EventIngress for MockIngress {
    fn recv(&mut self) -> Pin<Box<dyn Future<Output = Option<Event>> + Send + '_>> {
        Box::pin(self.rx.recv())
    }
}

// =============================================================================
// Multi-source ingress combiner
// =============================================================================

/// Combines multiple EventIngress sources into one stream
pub struct CombinedIngress {
    rx: mpsc::Receiver<Event>,
}

impl CombinedIngress {
    /// Create a combined ingress from multiple sources
    pub fn new(mut sources: Vec<Box<dyn EventIngress>>) -> Self {
        let (tx, rx) = mpsc::channel(1024);
        
        for mut source in sources.drain(..) {
            let tx = tx.clone();
            tokio::spawn(async move {
                while let Some(event) = source.recv().await {
                    if tx.send(event).await.is_err() {
                        break;
                    }
                }
            });
        }
        
        Self { rx }
    }
}

impl EventIngress for CombinedIngress {
    fn recv(&mut self) -> Pin<Box<dyn Future<Output = Option<Event>> + Send + '_>> {
        Box::pin(self.rx.recv())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_ingress_counter() {
        let config = MockConfig {
            rate: 0, // As fast as possible
            sources: vec![MockSource {
                name: "test".to_string(),
                pattern: MockPattern::Counter,
            }],
            limit: Some(10),
        };

        let mut ingress = MockIngress::new(config);
        let mut count = 0;

        while let Some(event) = ingress.recv().await {
            assert_eq!(event.source, "test");
            assert_eq!(event.payload["seq"], count);
            count += 1;
        }

        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn test_mock_ingress_state_machine() {
        let config = MockConfig {
            rate: 0,
            sources: vec![MockSource {
                name: "dut1".to_string(),
                pattern: MockPattern::StateMachine {
                    states: vec!["init".to_string(), "ready".to_string(), "done".to_string()],
                },
            }],
            limit: Some(6),
        };

        let mut ingress = MockIngress::new(config);
        let mut events = Vec::new();

        while let Some(event) = ingress.recv().await {
            events.push(event.payload["state"].as_str().unwrap().to_string());
        }

        assert_eq!(events, vec!["init", "ready", "done", "init", "ready", "done"]);
    }

    #[tokio::test]
    async fn test_mock_ingress_can_frame() {
        let config = MockConfig {
            rate: 0,
            sources: vec![MockSource {
                name: "dut1".to_string(),
                pattern: MockPattern::ProtocolFrame,
            }],
            limit: Some(5),
        };

        let mut ingress = MockIngress::new(config);

        let event = ingress.recv().await.unwrap();
        assert_eq!(event.payload["type"], "protocol");
        assert!(event.payload["id"].is_number());
        assert!(event.payload["data"].is_array());
    }

    #[tokio::test]
    async fn test_combined_ingress() {
        let source1 = Box::new(MockIngress::new(MockConfig {
            rate: 0,
            sources: vec![MockSource {
                name: "src1".to_string(),
                pattern: MockPattern::Counter,
            }],
            limit: Some(5),
        }));

        let source2 = Box::new(MockIngress::new(MockConfig {
            rate: 0,
            sources: vec![MockSource {
                name: "src2".to_string(),
                pattern: MockPattern::Counter,
            }],
            limit: Some(5),
        }));

        let mut combined = CombinedIngress::new(vec![source1, source2]);
        let mut count = 0;

        while let Some(_event) = combined.recv().await {
            count += 1;
            if count >= 10 {
                break;
            }
        }

        assert_eq!(count, 10);
    }
}
