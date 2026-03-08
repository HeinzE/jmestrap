//! MQTT event ingress
//!
//! Subscribes to an MQTT broker and converts published messages to Events.
//! Enabled with `--features mqtt`.
//!
//! # Topic mapping
//!
//! The source (shard key) is extracted from a configurable MQTT topic
//! segment (`source_segment`).  Segment 0 is typically the category
//! (e.g. `iothub`, `can0`) — JMESTrap ignores it.  Remaining segments
//! are available for external tooling (e.g. MQTT Explorer).
//!
//! ```text
//! Default (source_segment=1):
//!
//!   Segment index:   0            1
//!   MQTT topic:   category /   dutA
//!                              ↑
//!                            source
//!
//! Namespaced (source_segment=1):
//!
//!   Segment index:     0          1          2
//!   MQTT topic:    stations /  dutA   /  (ignored)
//!                              ↑
//!                            source
//! ```
//!
//! If the segment index is out of range for a given message, the value
//! falls back to `"unknown"`.
//!
//! # Example
//!
//! ```bash
//! cargo build --release --features mqtt
//!
//! # Default: segment 1 = source
//! jmestrap --mqtt localhost --mqtt-sub "sensors/#"
//!
//! # Custom source segment
//! jmestrap --mqtt localhost --mqtt-sub "stations/#" \
//!     --mqtt-source-segment 1
//! ```

use crate::ingress::{Event, EventIngress};
use rumqttc::{AsyncClient, Event as MqttEvent, MqttOptions, Packet, QoS};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::mpsc;

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for MQTT ingress
#[derive(Debug, Clone, Deserialize)]
pub struct MqttIngressConfig {
    /// MQTT client ID (must be unique per connection)
    pub client_id: String,
    /// Broker hostname or IP
    pub host: String,
    /// Broker port (default: 1883)
    #[serde(default = "default_port")]
    pub port: u16,
    /// Topic filters to subscribe to (e.g. ["sensors/#", "alerts/#"])
    #[serde(default = "default_subscribe")]
    pub subscribe: Vec<String>,
    /// Which topic segment is the jmestrap source (default: 1)
    #[serde(default = "default_source_segment")]
    pub source_segment: usize,
}

fn default_port() -> u16 {
    1883
}

fn default_subscribe() -> Vec<String> {
    vec!["#".to_string()]
}

fn default_source_segment() -> usize {
    1
}

// =============================================================================
// MqttIngress
// =============================================================================

/// MQTT subscriber ingress
pub struct MqttIngress {
    rx: mpsc::Receiver<Event>,
    _client: AsyncClient,
}

impl MqttIngress {
    /// Connect to an MQTT broker and subscribe to configured topics.
    pub async fn connect(config: MqttIngressConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let mut opts = MqttOptions::new(&config.client_id, &config.host, config.port);
        opts.set_keep_alive(Duration::from_secs(30));

        // Channel capacity: broker-side buffering handles bursts;
        // this is the rumqttc internal event queue depth.
        let (client, mut eventloop) = AsyncClient::new(opts, 256);

        for topic in &config.subscribe {
            client.subscribe(topic, QoS::AtMostOnce).await?;
        }

        eprintln!(
            "[mqtt] Connected to {}:{}, subscribed to {:?}",
            config.host, config.port, config.subscribe
        );

        let (tx, rx) = mpsc::channel(1024);
        let source_seg = config.source_segment;

        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(MqttEvent::Incoming(Packet::Publish(publish))) => {
                        if let Some(event) = parse_mqtt_message(
                            &publish.topic,
                            &publish.payload,
                            source_seg,
                        ) {
                            if tx.send(event).await.is_err() {
                                break; // Receiver dropped
                            }
                        }
                    }
                    Ok(_) => {} // ConnAck, PingResp, SubAck, etc — ignore
                    Err(e) => {
                        eprintln!("[mqtt] Connection error: {}, reconnecting...", e);
                        // rumqttc handles reconnection automatically;
                        // just keep polling the eventloop.
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
            eprintln!("[mqtt] Event loop exited");
        });

        Ok(Self { rx, _client: client })
    }
}

impl EventIngress for MqttIngress {
    fn recv(&mut self) -> Pin<Box<dyn Future<Output = Option<Event>> + Send + '_>> {
        Box::pin(self.rx.recv())
    }
}

// =============================================================================
// MQTT topic → Event mapping
// =============================================================================

/// Parse an MQTT publish message into a jmestrap Event.
///
/// Extracts source from a configurable segment index.
/// Falls back to "unknown" when the segment index is out of range.
fn parse_mqtt_message(
    mqtt_topic: &str,
    payload: &[u8],
    source_segment: usize,
) -> Option<Event> {
    let json: JsonValue = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[mqtt] JSON parse error on '{}': {}", mqtt_topic, e);
            return None;
        }
    };

    let segments: Vec<&str> = mqtt_topic.split('/').collect();

    let source = segments
        .get(source_segment)
        .unwrap_or(&"unknown")
        .to_string();

    Some(Event {
        source,
        payload: json,
    })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_two_segments_default() {
        let event = parse_mqtt_message(
            "sensors/node_1",
            br#"{"value": 42}"#,
            1,
        )
        .unwrap();
        assert_eq!(event.source, "node_1");
        assert_eq!(event.payload["value"], 42);
    }

    #[test]
    fn test_parse_three_segments_default() {
        let event = parse_mqtt_message(
            "sensors/node_1/temperature",
            br#"{"value": 22.5}"#,
            1,
        )
        .unwrap();
        assert_eq!(event.source, "node_1");
    }

    #[test]
    fn test_parse_single_segment() {
        let event = parse_mqtt_message(
            "heartbeat",
            br#"{"ts": 1234}"#,
            1,
        )
        .unwrap();
        assert_eq!(event.source, "unknown");
    }

    #[test]
    fn test_parse_invalid_json() {
        let result = parse_mqtt_message("sensors/node_1", b"not json", 1);
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_custom_source_segment() {
        let event = parse_mqtt_message(
            "nodes/node_4/can",
            br#"{"can_id": "0x61e"}"#,
            1,
        )
        .unwrap();
        assert_eq!(event.source, "node_4");
    }

    #[test]
    fn test_parse_source_segment_out_of_range() {
        let event = parse_mqtt_message(
            "heartbeat",
            br#"{"ts": 1234}"#,
            2,
        )
        .unwrap();
        assert_eq!(event.source, "unknown");
    }

    // Config deserialization

    #[test]
    fn test_config_defaults() {
        let json = r#"{
            "client_id": "test",
            "host": "localhost"
        }"#;
        let config: MqttIngressConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.port, 1883);
        assert_eq!(config.subscribe, vec!["#"]);
        assert_eq!(config.source_segment, 1);
    }

    #[test]
    fn test_config_custom() {
        let json = r#"{
            "client_id": "test",
            "host": "broker.local",
            "port": 8883,
            "subscribe": ["nodes/#"],
            "source_segment": 2
        }"#;
        let config: MqttIngressConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.host, "broker.local");
        assert_eq!(config.port, 8883);
        assert_eq!(config.subscribe, vec!["nodes/#"]);
        assert_eq!(config.source_segment, 2);
    }
}
