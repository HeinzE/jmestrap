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

#[derive(Debug)]
struct Args {
    port: u16,
    bind: String,
    ttl: u64,
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
            ttl: 3600,
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
    match parse_args_from(&args) {
        Ok(args) => args,
        Err(ParseArgsError::Help) => {
            print_usage();
            std::process::exit(0);
        }
        Err(ParseArgsError::Invalid(msg)) => {
            eprintln!("{}", msg);
            print_usage();
            std::process::exit(1);
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ParseArgsError {
    Help,
    Invalid(String),
}

fn parse_args_from(args: &[String]) -> Result<Args, ParseArgsError> {
    let mut result = Args::default();

    fn fail_arg<T>(flag: &str, msg: &str) -> Result<T, ParseArgsError> {
        Err(ParseArgsError::Invalid(format!("{}: {}", flag, msg)))
    }

    fn next_value(
        args: &[String],
        i: &mut usize,
        flag: &str,
    ) -> Result<String, ParseArgsError> {
        *i += 1;
        if *i >= args.len() {
            return fail_arg(flag, "missing value");
        }
        Ok(args[*i].clone())
    }

    fn parse_u16(flag: &str, raw: &str) -> Result<u16, ParseArgsError> {
        match raw.parse::<u16>() {
            Ok(v) => Ok(v),
            Err(_) => fail_arg(flag, &format!("invalid integer: {}", raw)),
        }
    }

    fn parse_u64(flag: &str, raw: &str) -> Result<u64, ParseArgsError> {
        match raw.parse::<u64>() {
            Ok(0) => fail_arg(flag, "must be positive"),
            Ok(v) => Ok(v),
            Err(_) => fail_arg(flag, &format!("invalid integer: {}", raw)),
        }
    }

    #[cfg(feature = "mqtt")]
    fn parse_usize(flag: &str, raw: &str) -> Result<usize, ParseArgsError> {
        match raw.parse::<usize>() {
            Ok(v) => Ok(v),
            Err(_) => fail_arg(flag, &format!("invalid integer: {}", raw)),
        }
    }

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" | "-p" => {
                let value = next_value(args, &mut i, "--port")?;
                result.port = parse_u16("--port", &value)?;
            }
            "--bind" | "-b" => {
                result.bind = next_value(args, &mut i, "--bind")?;
            }
            #[cfg(feature = "sse")]
            "--sse" | "-s" => {
                result
                    .sse_endpoints
                    .push(next_value(args, &mut i, "--sse")?);
            }
            #[cfg(feature = "sse")]
            "--sse-source-field" => {
                result.sse_source_field =
                    next_value(args, &mut i, "--sse-source-field")?;
            }
            #[cfg(feature = "mqtt")]
            "--mqtt" | "-m" => {
                result.mqtt_broker = Some(next_value(args, &mut i, "--mqtt")?);
            }
            #[cfg(feature = "mqtt")]
            "--mqtt-port" => {
                let value = next_value(args, &mut i, "--mqtt-port")?;
                result.mqtt_port = parse_u16("--mqtt-port", &value)?;
            }
            #[cfg(feature = "mqtt")]
            "--mqtt-subscribe" | "--mqtt-sub" => {
                result
                    .mqtt_subscribe
                    .push(next_value(args, &mut i, "--mqtt-sub")?);
            }
            #[cfg(feature = "mqtt")]
            "--mqtt-source-segment" => {
                let value = next_value(args, &mut i, "--mqtt-source-segment")?;
                result.mqtt_source_segment = parse_usize("--mqtt-source-segment", &value)?;
            }
            "--ttl" => {
                let value = next_value(args, &mut i, "--ttl")?;
                result.ttl = parse_u64("--ttl", &value)?;
            }
            "--help" | "-h" => {
                return Err(ParseArgsError::Help);
            }
            _ => {
                return Err(ParseArgsError::Invalid(format!(
                    "Unknown argument: {}",
                    args[i]
                )));
            }
        }
        i += 1;
    }
    Ok(result)
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
    eprintln!("      --ttl <SECONDS>     Recording TTL in seconds (default: 3600)");
    eprintln!("  -h, --help              Show this help");
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() {
    let args = parse_args();
    let addr = format!("{}:{}", args.bind, args.port);
    let state = Arc::new(AppState::new().with_ttl_secs(args.ttl));
    let _reaper = crate::core::spawn_reaper(Arc::clone(&state), 60);

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

            let url = format!("mqtt://{}:{}", host, args.mqtt_port);
            eprintln!("  mqtt {} (topics: {:?})", url, subscribe);

            let config = MqttIngressConfig {
                client_id: format!("jmestrap_{}", std::process::id()),
                host: host.clone(),
                port: args.mqtt_port,
                subscribe,
                source_segment: args.mqtt_source_segment,
            };

            match MqttIngress::connect(config).await {
                Ok(ingress) => {
                    let _task = crate::core::spawn_ingress_processor(
                        Box::new(ingress),
                        Arc::clone(&state),
                    );
                }
                Err(e) => {
                    eprintln!("[mqtt] Failed to connect to {}: {}", url, e);
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

    eprintln!("JmesTrap listening on http://{addr}  (ttl: {}s)", args.ttl);
    eprintln!();
    eprintln!("  GET    /ping");
    eprintln!("  GET    /ui");
    eprintln!("  POST   /recordings");
    eprintln!("  GET    /recordings");
    eprintln!("  GET    /recordings/{{ref}}?timeout=N");
    eprintln!("  POST   /recordings/{{ref}}/stop");
    eprintln!("  DELETE /recordings/{{ref}}");
    eprintln!("  GET    /sources");
    eprintln!("  POST   /events/{{source}}");

    axum::serve(listener, app).await.unwrap();
}

#[cfg(test)]
mod cli_tests {
    use super::*;

    fn parse(parts: &[&str]) -> Result<Args, ParseArgsError> {
        let args: Vec<String> = parts.iter().map(|s| s.to_string()).collect();
        parse_args_from(&args)
    }

    #[test]
    fn parse_defaults() {
        let args = parse(&["jmestrap"]).unwrap();
        assert_eq!(args.port, 9000);
        assert_eq!(args.bind, "127.0.0.1");
    }

    #[test]
    fn parse_port_and_bind() {
        let args = parse(&["jmestrap", "--port", "9100", "--bind", "0.0.0.0"])
            .unwrap();
        assert_eq!(args.port, 9100);
        assert_eq!(args.bind, "0.0.0.0");
    }

    #[test]
    fn parse_short_flags() {
        let args = parse(&["jmestrap", "-p", "9101", "-b", "localhost"]).unwrap();
        assert_eq!(args.port, 9101);
        assert_eq!(args.bind, "localhost");
    }

    #[test]
    fn parse_help_returns_help_error() {
        let err = parse(&["jmestrap", "--help"]).unwrap_err();
        assert_eq!(err, ParseArgsError::Help);
    }

    #[test]
    fn parse_unknown_argument_returns_error() {
        let err = parse(&["jmestrap", "--bogus"]).unwrap_err();
        assert_eq!(
            err,
            ParseArgsError::Invalid("Unknown argument: --bogus".to_string())
        );
    }

    #[test]
    fn parse_missing_port_value_returns_error() {
        let err = parse(&["jmestrap", "--port"]).unwrap_err();
        assert_eq!(
            err,
            ParseArgsError::Invalid("--port: missing value".to_string())
        );
    }

    #[test]
    fn parse_invalid_port_returns_error() {
        let err = parse(&["jmestrap", "--port", "abc"]).unwrap_err();
        assert_eq!(
            err,
            ParseArgsError::Invalid("--port: invalid integer: abc".to_string())
        );
    }

    #[test]
    fn parse_ttl_flag() {
        let args = parse(&["jmestrap", "--ttl", "300"]).unwrap();
        assert_eq!(args.ttl, 300);
    }

    #[test]
    fn parse_ttl_default() {
        let args = parse(&["jmestrap"]).unwrap();
        assert_eq!(args.ttl, 3600);
    }

    #[test]
    fn parse_ttl_invalid() {
        let err = parse(&["jmestrap", "--ttl", "abc"]).unwrap_err();
        assert_eq!(
            err,
            ParseArgsError::Invalid("--ttl: invalid integer: abc".to_string())
        );
    }

    #[test]
    fn parse_ttl_zero_rejected() {
        let err = parse(&["jmestrap", "--ttl", "0"]).unwrap_err();
        assert_eq!(
            err,
            ParseArgsError::Invalid("--ttl: must be positive".to_string())
        );
    }
}
