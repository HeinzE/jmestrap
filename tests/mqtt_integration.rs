#![cfg(feature = "mqtt")]

use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};
use serde_json::{Value as JsonValue, json};
use std::env;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

struct ChildGuard {
    child: Child,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn find_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("read local addr").port()
}

fn http_json(port: u16, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to server");
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );

    stream
        .write_all(request.as_bytes())
        .expect("write request");

    let mut raw = String::new();
    stream.read_to_string(&mut raw).expect("read response");

    let (head, body) = raw.split_once("\r\n\r\n").expect("http response split");
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .expect("status line")
        .parse::<u16>()
        .expect("status code parse");

    (status, body.to_string())
}

fn wait_for_ping(port: u16, timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) {
            let req = "GET /ping HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
            if stream.write_all(req.as_bytes()).is_ok() {
                let mut raw = String::new();
                if stream.read_to_string(&mut raw).is_ok() && raw.contains("200 OK") {
                    return;
                }
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("server did not become ready within timeout");
}

fn mqtt_integration_enabled() -> bool {
    matches!(
        env::var("JMESTRAP_RUN_MQTT_INTEGRATION").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
    )
}

async fn publish_once(
    host: &str,
    port: u16,
    topic: &str,
    payload: JsonValue,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut mqtt = MqttOptions::new(
        format!("jmestrap_mqtt_test_pub_{}", std::process::id()),
        host,
        port,
    );
    mqtt.set_keep_alive(Duration::from_secs(5));

    let (client, mut event_loop): (AsyncClient, EventLoop) = AsyncClient::new(mqtt, 10);
    let poller = tokio::spawn(async move {
        for _ in 0..20 {
            let _ = event_loop.poll().await;
        }
    });

    client
        .publish(topic, QoS::AtLeastOnce, false, payload.to_string())
        .await?;
    tokio::time::sleep(Duration::from_millis(250)).await;
    drop(client);
    let _ = poller.await;
    Ok(())
}

#[tokio::test]
async fn mqtt_broker_to_recording_happy_path() {
    if !mqtt_integration_enabled() {
        eprintln!(
            "skipping mqtt integration test: set JMESTRAP_RUN_MQTT_INTEGRATION=1 to enable"
        );
        return;
    }

    let mqtt_host = env::var("JMESTRAP_MQTT_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let mqtt_port: u16 = env::var("JMESTRAP_MQTT_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1883);

    let api_port = find_free_port();
    let bin = env!("CARGO_BIN_EXE_jmestrap");

    let child = Command::new(bin)
        .arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(api_port.to_string())
        .arg("--mqtt")
        .arg(&mqtt_host)
        .arg("--mqtt-port")
        .arg(mqtt_port.to_string())
        .arg("--mqtt-sub")
        .arg("sensors/#")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn jmestrap with mqtt ingress");
    let _guard = ChildGuard { child };

    wait_for_ping(api_port, Duration::from_secs(5));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let create_body =
        r#"{"sources":["station_01"],"until":{"type":"order","predicates":["event=='go'"]}}"#;
    let (status, body) = http_json(api_port, "POST", "/recordings", Some(create_body));
    assert_eq!(status, 201);
    let created: JsonValue = serde_json::from_str(&body).expect("parse create JSON");
    let reference = created["reference"]
        .as_u64()
        .expect("recording reference present");

    publish_once(
        &mqtt_host,
        mqtt_port,
        "sensors/station_01/telemetry",
        json!({"event":"go","value":1}),
    )
    .await
    .expect("publish mqtt event");

    let (status, body) = http_json(
        api_port,
        "GET",
        &format!("/recordings/{reference}?timeout=2.0"),
        None,
    );
    assert_eq!(status, 200);
    let recording: JsonValue = serde_json::from_str(&body).expect("parse recording JSON");
    assert_eq!(recording["finished"], true);
}
