//! Minimal Rust REST client example for jmestrap.
//!
//! This is intentionally small and std-only on HTTP transport
//! (no external HTTP client crate). It is meant as a learning
//! reference for request/response handling in Rust.
//!
//! Run with:
//!   cargo run --example minimal_rust_client

use serde_json::{json, Value as JsonValue};
use std::io::{Read, Write};
use std::net::TcpStream;

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    body: String,
}

fn http_json(method: &str, path: &str, body: Option<&str>) -> Result<HttpResponse, String> {
    let mut stream =
        TcpStream::connect("127.0.0.1:9000").map_err(|e| format!("connect failed: {e}"))?;

    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );

    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("write failed: {e}"))?;

    let mut raw = String::new();
    stream
        .read_to_string(&mut raw)
        .map_err(|e| format!("read failed: {e}"))?;

    let (head, body) = raw
        .split_once("\r\n\r\n")
        .ok_or_else(|| "invalid HTTP response".to_string())?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| "missing status line".to_string())?
        .parse::<u16>()
        .map_err(|e| format!("invalid status: {e}"))?;

    Ok(HttpResponse {
        status,
        body: body.to_string(),
    })
}

fn parse_json(body: &str) -> Result<JsonValue, String> {
    serde_json::from_str(body).map_err(|e| format!("invalid JSON body: {e}"))
}

fn main() -> Result<(), String> {
    // 1) Health check
    let ping = http_json("GET", "/ping", None)?;
    if ping.status != 200 {
        return Err(format!("/ping failed: {} {}", ping.status, ping.body));
    }
    println!("ping: {}", ping.body);

    // 2) Start recording
    let start_body = json!({
        "sources": ["rust_client_demo"],
        "until": {
            "type": "order",
            "predicates": ["event == 'start'", "event == 'done'"]
        }
    })
    .to_string();

    let start = http_json("POST", "/recordings", Some(&start_body))?;
    if start.status != 201 {
        return Err(format!(
            "create recording failed: {} {}",
            start.status, start.body
        ));
    }
    let start_json = parse_json(&start.body)?;
    let reference = start_json["reference"]
        .as_u64()
        .ok_or_else(|| "missing recording reference".to_string())?;
    println!("recording ref: {reference}");

    // 3) Inject events via REST ingress
    let events = [
        json!({"event": "start", "value": 1}),
        json!({"event": "noise", "value": 2}),
        json!({"event": "done", "value": 3}),
    ];
    for event in events {
        let r = http_json(
            "POST",
            "/events/rust_client_demo",
            Some(&event.to_string()),
        )?;
        if r.status != 202 {
            return Err(format!("inject failed: {} {}", r.status, r.body));
        }
    }

    // 4) Fetch result
    let result = http_json("GET", &format!("/recordings/{reference}?timeout=2"), None)?;
    if result.status != 200 {
        return Err(format!(
            "get recording failed: {} {}",
            result.status, result.body
        ));
    }
    println!("recording result: {}", result.body);

    Ok(())
}
