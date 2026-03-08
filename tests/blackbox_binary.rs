use serde_json::Value as JsonValue;
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

    stream.write_all(request.as_bytes()).expect("write request");

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

#[test]
fn binary_happy_path_over_http() {
    let port = find_free_port();
    let bin = env!("CARGO_BIN_EXE_jmestrap");

    let child = Command::new(bin)
        .arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn jmestrap binary");
    let _guard = ChildGuard { child };

    wait_for_ping(port, Duration::from_secs(5));

    let (status, body) = http_json(port, "GET", "/ping", None);
    assert_eq!(status, 200);
    let ping: JsonValue = serde_json::from_str(&body).expect("parse ping JSON");
    assert_eq!(ping["status"], "ok");

    let create_body =
        r#"{"sources":["bb_src"],"until":{"type":"order","predicates":["event=='go'"]}}"#;
    let (status, body) = http_json(port, "POST", "/recordings", Some(create_body));
    assert_eq!(status, 201);
    let created: JsonValue = serde_json::from_str(&body).expect("parse create JSON");
    let reference = created["reference"]
        .as_u64()
        .expect("recording reference present");

    let (status, _) = http_json(
        port,
        "POST",
        "/events/bb_src",
        Some(r#"{"event":"go"}"#),
    );
    assert_eq!(status, 202);

    let (status, body) = http_json(
        port,
        "GET",
        &format!("/recordings/{reference}?timeout=0.3"),
        None,
    );
    assert_eq!(status, 200);
    let recording: JsonValue = serde_json::from_str(&body).expect("parse recording JSON");
    assert_eq!(recording["finished"], true);
}
