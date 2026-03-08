mod control;
mod core;
mod ingress;
mod predicates;
mod rest;
mod stress_tests;

#[cfg(feature = "sse")]
mod sse_ingress;
#[cfg(feature = "mqtt")]
mod mqtt_ingress;

use crate::core::AppState;
use std::sync::Arc;

// =============================================================================
// CLI
// =============================================================================

struct Args {
    port: u16,
    bind: String,
    #[cfg(feature = "sse")]
    sse_endpoints: Vec<String>,
    #[cfg(feature = "sse")]
    sse_source_field: String,
    #[cfg(feature = "mqtt")]
    mqtt_broker: Option<String>,
    #[cfg(feature = "mqtt")]
    mqtt_port: u16,
    #[cfg(feature = "mqtt")]
    mqtt_subscribe: Vec<String>,
    #[cfg(feature = "mqtt")]
    mqtt_source_segment: usize,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            port: 9000,
            bind: "127.0.0.1".to_string(),
            #[cfg(feature = "sse")]
            sse_endpoints: Vec::new(),
            #[cfg(feature = "sse")]
            sse_source_field: "sourceId".to_string(),
            #[cfg(feature = "mqtt")]
            mqtt_broker: None,
            #[cfg(feature = "mqtt")]
            mqtt_port: 1883,
            #[cfg(feature = "mqtt")]
            mqtt_subscribe: Vec::new(),
            #[cfg(feature = "mqtt")]
            mqtt_source_segment: 1,
        }
    }
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    let mut result = Args::default();

    fn fail_arg(flag: &str, msg: &str) -> ! {
        eprintln!("{}: {}", flag, msg);
        print_usage();
        std::process::exit(1);
    }

    fn next_value(args: &[String], i: &mut usize, flag: &str) -> String {
        *i += 1;
        if *i >= args.len() {
            fail_arg(flag, "missing value");
        }
        args[*i].clone()
    }

    fn parse_u16(flag: &str, raw: &str) -> u16 {
        match raw.parse::<u16>() {
            Ok(v) => v,
            Err(_) => fail_arg(flag, &format!("invalid integer: {}", raw)),
        }
    }

    #[cfg(feature = "mqtt")]
    fn parse_usize(flag: &str, raw: &str) -> usize {
        match raw.parse::<usize>() {
            Ok(v) => v,
            Err(_) => fail_arg(flag, &format!("invalid integer: {}", raw)),
        }
    }

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" | "-p" => {
                let value = next_value(&args, &mut i, "--port");
                result.port = parse_u16("--port", &value);
            }
            "--bind" | "-b" => {
                result.bind = next_value(&args, &mut i, "--bind");
            }
            #[cfg(feature = "sse")]
            "--sse" | "-s" => {
                result.sse_endpoints.push(next_value(&args, &mut i, "--sse"));
            }
            #[cfg(feature = "sse")]
            "--sse-source-field" => {
                result.sse_source_field = next_value(&args, &mut i, "--sse-source-field");
            }
            #[cfg(feature = "mqtt")]
            "--mqtt" | "-m" => {
                result.mqtt_broker = Some(next_value(&args, &mut i, "--mqtt"));
            }
            #[cfg(feature = "mqtt")]
            "--mqtt-port" => {
                let value = next_value(&args, &mut i, "--mqtt-port");
                result.mqtt_port = parse_u16("--mqtt-port", &value);
            }
            #[cfg(feature = "mqtt")]
            "--mqtt-subscribe" | "--mqtt-sub" => {
                result
                    .mqtt_subscribe
                    .push(next_value(&args, &mut i, "--mqtt-sub"));
            }
            #[cfg(feature = "mqtt")]
            "--mqtt-source-segment" => {
                let value = next_value(&args, &mut i, "--mqtt-source-segment");
                result.mqtt_source_segment = parse_usize("--mqtt-source-segment", &value);
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                print_usage();
                std::process::exit(1);
            }
        }
        i += 1;
    }
    result
}

fn print_usage() {
    eprintln!("Usage: jmestrap [OPTIONS]");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  -p, --port <PORT>       Listen port (default: 9000)");
    eprintln!("  -b, --bind <ADDR>       Bind address (default: 127.0.0.1)");
    #[cfg(feature = "sse")]
    {
        eprintln!("  -s, --sse <URL>         SSE endpoint to subscribe to (can repeat)");
        eprintln!("      --sse-source-field <F>  JSON field for source (default: sourceId)");
    }
    #[cfg(feature = "mqtt")]
    {
        eprintln!("  -m, --mqtt <HOST>       MQTT broker host");
        eprintln!("      --mqtt-port <PORT>  MQTT broker port (default: 1883)");
        eprintln!("      --mqtt-sub <TOPIC>  MQTT topic to subscribe to (can repeat)");
        eprintln!("      --mqtt-source-segment <N>  Topic segment for source (default: 1)");
    }
    eprintln!("  -h, --help              Show this help");
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() {
    let args = parse_args();
    let addr = format!("{}:{}", args.bind, args.port);
    let state = Arc::new(AppState::new());

    // Start SSE ingress tasks if configured
    #[cfg(feature = "sse")]
    {
        use crate::sse_ingress::{SseIngress, SseIngressConfig};

        for url in &args.sse_endpoints {
            let config = SseIngressConfig {
                url: url.clone(),
                source_field: args.sse_source_field.clone(),
                default_source: "unknown".to_string(),
            };

            match SseIngress::connect(config).await {
                Ok(ingress) => {
                    eprintln!("  sse  {}", url);
                    let _task = crate::core::spawn_ingress_processor(
                        Box::new(ingress),
                        Arc::clone(&state),
                    );
                }
                Err(e) => {
                    eprintln!("[sse] Failed to connect to {}: {}", url, e);
                }
            }
        }
    }

    // Start MQTT ingress if configured
    #[cfg(feature = "mqtt")]
    {
        use crate::mqtt_ingress::{MqttIngress, MqttIngressConfig};

        if let Some(ref host) = args.mqtt_broker {
            let subscribe = if args.mqtt_subscribe.is_empty() {
                vec!["#".to_string()] // Subscribe to all if no topics specified
            } else {
                args.mqtt_subscribe.clone()
            };

            let config = MqttIngressConfig {
                client_id: format!("jmestrap_{}", std::process::id()),
                host: host.clone(),
                port: args.mqtt_port,
                subscribe: subscribe.clone(),
                source_segment: args.mqtt_source_segment,
            };

            match MqttIngress::connect(config).await {
                Ok(ingress) => {
                    let url = format!("mqtt://{}:{}", host, args.mqtt_port);
                    eprintln!("  mqtt {} (topics: {:?})", url, subscribe);
                    let _task = crate::core::spawn_ingress_processor(
                        Box::new(ingress),
                        Arc::clone(&state),
                    );
                }
                Err(e) => {
                    eprintln!("[mqtt] Failed to connect to {}:{}: {}", host, args.mqtt_port, e);
                }
            }
        }
    }

    let app = rest::router(state);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Error: cannot bind to {addr}: {e}");
            std::process::exit(1);
        });

    eprintln!("JmesTrap listening on http://{addr}");
    eprintln!();
    eprintln!("  GET    /ping");
    eprintln!("  POST   /recordings");
    eprintln!("  GET    /recordings");
    eprintln!("  GET    /recordings/{{ref}}?timeout=N");
    eprintln!("  POST   /recordings/{{ref}}/stop");
    eprintln!("  DELETE /recordings/{{ref}}");
    eprintln!("  GET    /sources");
    eprintln!("  POST   /events/{{source}}");

    axum::serve(listener, app).await.unwrap();
}
