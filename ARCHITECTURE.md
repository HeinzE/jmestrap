# Architecture

## Module Layout

```
main.rs          CLI parsing, ingress startup, axum server startup
rest.rs          HTTP handlers (axum) — thin adapter over RecordingControl
control.rs       RecordingControl trait, request/response types
core.rs          AppState, Recording, source sharding, event processing
ingress.rs       EventIngress trait, MockIngress
mqtt_ingress.rs  MqttIngress, topic segment parsing (--features mqtt)
sse_ingress.rs   SseIngress, JSON field extraction (--features sse)
predicates.rs    Jmes, JmesUntil (Order, AnyOrder), thread-local cache
stress_tests.rs  Throughput and sharding stress tests
```

## Design Principles

1. **Traits for real variation.** `EventIngress` has two implementations
   (SSE, MQTT) with genuinely different wire protocols and identity
   extraction. `RecordingControl` separates business logic from HTTP.

2. **Feature flags for optional deps.** SSE needs `reqwest`, MQTT needs
   `rumqttc`. Core compiles with zero network client dependencies.

3. **No premature abstraction.** Source extraction is per-ingress
   because SSE and MQTT have fundamentally different identity models
   (JSON field vs topic hierarchy).

## Runtime Data Flow

```
  EventIngress (MQTT / SSE / REST inject)
         │
  spawn_ingress_processor()
         │
  AppState.process_event()
         │
  ┌──────┴──────┐
  │             │
source shard  global shard
(per-source   (recordings with
 lock)         sources=[])
  │             │
Recordings   Recordings
```

Events from all transports merge by source name. If MQTT and SSE both
produce `source="station_north"`, their events enter the same shard.

### Source Sharding

Recordings are sharded by source to eliminate cross-source lock contention.
100 sources means 100 independent shards.

```
AppState
├── shards: RwLock<HashMap<String, Arc<RwLock<Recordings>>>>
│   ├── "station_north" → Recordings { rec1, rec3 }
│   ├── "station_south" → Recordings { rec2 }
│   └── "station_east"  → Recordings { rec4, rec5 }
├── global: Arc<RwLock<Recordings>>     ← sources=[] recordings
├── counter: AtomicU64                  ← lock-free ref allocator
├── ref_index: RwLock<HashMap<ref, sources>>
└── source_stats: std::sync::RwLock<HashMap<String, Mutex<SourceStatsInner>>>
                                        ← per-source counters
                                          (outer RwLock read-locked on updates,
                                           inner Mutex per-source)
```

| Recording sources | Placement | Events checked |
|---|---|---|
| `["station_north"]` | `shards["station_north"]` | Only that source |
| `[]` (match all) | `global` | All events |
| `["s1", "s2"]` (multi) | `global` | All events |

Single-source recordings (the common case) take the fast path: one shard,
one lock, no contention with other sources.

Events without a matching recording are silently dropped. No buffering or
replay. Recordings must be started before events of interest arrive.

### Ingress Identity Models

| Aspect | MQTT | SSE |
|--------|------|-----|
| Source | Topic segment (`category/{source}/...`) | JSON field (e.g. `sourceId`) |
| Pre-parse filter | Topic wildcards (`sensors/#`) | None (parse all JSON) |
| Connection | TCP via broker (auto-reconnect) | HTTP long-lived GET |

## Control API

```
RecordingControl trait (control.rs)
├── start(StartRequest) → StartResponse
├── stop(ref) → StopResponse
├── get_events(GetEventsRequest) → EventsResponse
├── delete(ref) → DeleteResponse
├── list() → ListResponse
└── list_sources() → SourcesResponse

Implemented by: AppState
Consumed by: rest.rs
```

The REST layer is a thin adapter: deserialize, call trait method, serialize.
`ControlError` maps to HTTP status codes (404 for not found, 422 for invalid
predicates). Adding a different transport (gRPC, WebSocket) means writing
another thin adapter.

`RecordingInfo` (from `list`) includes `matching_expr`, `until` progress,
timestamps (`created_at_ms`, `first_event_at_ms`, `last_event_at_ms`,
`finished_at_ms`), and `events_evaluated` so dashboards can show recording
state without fetching each individually.

## Deployment Notes

Target: controlled LAN (lab network, Docker Compose, internal VPN). No HTTP
proxies between clients and server. Long-poll (`?timeout=N`) relies on
direct connections — intermediaries may impose idle timeouts that cut the
connection. For proxied deployments, clients should loop with short timeouts
(the `demo.html` pattern).

## Testing Strategy

### Layer Ownership

| Layer | Owns | Does not own |
|-------|------|--------------|
| Unit tests (`core.rs`, `control.rs`, `predicates.rs`, ingress parsers) | Recording semantics, predicates, source stats, shard behavior | Process startup, binary wiring |
| Route tests (`rest.rs`, Router + oneshot) | HTTP status mapping, request/response shape, route lifecycle | OS/process behavior |
| Black-box (`tests/blackbox_binary.rs`) | Spawned binary over real HTTP | Broad matrix/fuzz |
| Smoke (`smoke_rest_binary.sh`) | Build + endpoint reachability + one happy path | Deep semantic validation |

Each behavior has one primary test owner. Avoid mirrored assertions across layers.

### CI

Required (run on every push/PR):
- `cargo test --locked`
- `cargo test --locked --features sse,mqtt`
- `cargo test --locked --test blackbox_binary`

Optional (manual dispatch):
- MQTT integration with containerized Mosquitto

## Contributing

**PR expectations:**
- Scope changes to one concern.
- If behavior changes (request parsing, response keys, route shapes,
  matching semantics), document the impact.
- Add tests at the correct layer.

**Release checks:**
```bash
cargo test --locked
cargo test --locked --features sse,mqtt
cargo test --locked --test blackbox_binary
./smoke_rest_binary.sh
```

## Future Direction

- **Recording TTL / auto-cleanup** — bounded retention with a background
  reaper to avoid stale recording buildup.
- **SSE reconnect** — if SSE sees active use, add reconnect/resume behavior.
