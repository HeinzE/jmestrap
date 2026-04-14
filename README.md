# JMESTrap

Record events from live sources using event patterns built from
[JMESPath](https://jmespath.org/) predicates. When the completion pattern
is satisfied the recording finishes and the matched events are returned.

JMESTrap is transport-agnostic. Events arrive via MQTT, SSE, or direct
REST injection. The core sees one merged stream per source and evaluates
predicates against it.

> **Terminology:** each JMESPath expression is a *predicate* (boolean filter
> on one event). `order` and `any_order` compose predicates into *event
> patterns* that match across a stream of events.

## Build

```bash
cargo build --release                        # core only (REST inject)
cargo build --release --features mqtt        # + MQTT ingress
cargo build --release --features sse         # + SSE ingress
cargo build --release --features mqtt,sse    # both
```

Requires Rust 1.85+ (edition 2024).

## Quick Start

Start the server:

```bash
./target/release/jmestrap              # default TTL: 3600s
./target/release/jmestrap --ttl 7200   # 2-hour TTL
```

Create a recording that completes when it sees `event=='start'` followed
by `event=='done'`:

```bash
curl -s http://localhost:9000/recordings \
  -H 'Content-Type: application/json' \
  -d '{
    "sources": ["sensor_1"],
    "until": {
      "type": "order",
      "predicates": ["event == '\''start'\''", "event == '\''done'\''"]
    }
  }'
# → {"reference": 1}
```

Inject events (in a real deployment these come from MQTT or SSE):

```bash
curl -s -X POST http://localhost:9000/events/sensor_1 \
  -H 'Content-Type: application/json' -d '{"event":"start","value":1}'

curl -s -X POST http://localhost:9000/events/sensor_1 \
  -H 'Content-Type: application/json' -d '{"event":"done","value":2}'
```

Fetch the result (long-poll waits up to `timeout` seconds for completion):

```bash
curl -s 'http://localhost:9000/recordings/1?timeout=5'
# → {"reference":1, "status":"completed", "events":[...], "until":{...}}
```

Clean up:

```bash
curl -s -X DELETE http://localhost:9000/recordings/1
```

## Concepts

JMESTrap has one filtering mechanism: JMESPath expressions evaluated
against event payloads. There is no built-in event taxonomy or type
system — publishers put meaningful keys in the payload, and predicates
decide what matters.

**Events** have two parts: a *source* (who sent it) and a JSON *payload*
(the data). Sources are the only structural pre-filter — they act as
a natural shard boundary (one device, one station, one stream). All
events from a given source merge into one ordered stream.

**Recordings** watch one or more sources and collect events that pass an
optional `matching` filter. A recording finishes when its `until`
completion pattern is satisfied.

**Completion patterns** come in two forms:

- `order` — a *sequence pattern*: predicates must match in order across
  successive events. Each predicate is tried against incoming events;
  when it matches, evaluation advances to the next. The recording
  finishes when the last predicate matches.
- `any_order` — a *conjunction pattern*: all predicates must match, each
  consumed once, in any order. Each incoming event is tested against all
  unmatched predicates.

Predicates are JMESPath expressions evaluated against the event payload.
A predicate matches when the expression returns a truthy value.

**Sources** are auto-discovered. `GET /sources` returns known sources along
with per-source statistics. A source may appear either because it has had
events or because an active recording targets it.

## REST API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/ping` | Health check |
| `GET` | `/ui` | Embedded dashboard (HTML) |
| `POST` | `/recordings` | Create recording |
| `GET` | `/recordings` | List all recordings |
| `GET` | `/recordings/{ref}?timeout=N` | Get recording (long-poll) |
| `POST` | `/recordings/{ref}/stop` | Stop recording early |
| `DELETE` | `/recordings/{ref}` | Delete recording |
| `GET` | `/sources` | List observed sources and stats |
| `POST` | `/events/{source}` | Inject event |

## Recording Payload

```json
{
  "description": "login flow",
  "sources": ["sensor_1"],
  "matching": "type == 'protocol_frame'",
  "until": {
    "type": "order",
    "predicates": ["state == 'init'", "state == 'ready'"]
  }
}
```

- `description` — optional human-readable label for the recording.
- `sources` — which sources to watch. Empty array watches all sources.
- `matching` — JMESPath filter. Only matching events are recorded.
  Without `matching`, only events that satisfy `until` predicates are
  captured. Use `` `true` `` to record all events (the JMESPath literal
  `true` is unconditionally truthy).
- `until` — optional completion pattern (`order` or `any_order`).
  Without `until`, the recording runs until explicitly stopped.

### Common Pitfalls

A few behaviors surprise first-time users. None are bugs — they follow
from the semantics above — but they are worth calling out:

- **`sources: []` means *match all sources*, not *match none*.** An
  empty array is the wildcard. To watch nothing, don't create the
  recording. Wildcard recordings take a slower path (global shard)
  than single-source recordings.
- **Omitting `matching` records *only* events that satisfy `until`.**
  `{sources: ["dut1"], until: {...}}` looks like "record everything
  until X" but actually records almost nothing. To record every event
  up to the completion, set `` "matching": "`true`" ``.
- **`until` consumes the event before `matching` sees it.** When an
  event satisfies the current `until` predicate, it is tagged as an
  until-match and skips the `matching` filter. An event cannot be
  both a "completion" event and a captured event in the same
  recording — it is one or the other.

### Recording Response

Fetched recordings include timing and statistics:

- `created_at_ms`, `first_event_at_ms`, `last_event_at_ms`,
  `finished_at_ms` — Unix epoch milliseconds.
- `events_evaluated` — total events tested against this recording's
  predicates (including non-matches). Compare with `event_count` to
  gauge selectivity.

### Recording Lifecycle

Recordings are automatically cleaned up after a configurable TTL (default
3600 seconds, set with `--ttl`):

- **Completed/Stopped** recordings are deleted once `--ttl` seconds have
  elapsed since they finished.
- **Running** recordings that have not been fetched within `--ttl` seconds
  (idle) are also cleaned up.
- Fetching a recording (`GET /recordings/{ref}`) resets the idle clock,
  so actively polled recordings are never reaped.

A background reaper runs every 60 seconds to enforce these limits.

## Event Ingress

### REST

Direct injection via `POST /events/{source}`. Useful for testing,
scripting, and bridging from systems without native MQTT/SSE.

### MQTT

MQTT is the primary ingress for production deployments. The source
(shard key) is extracted from a configurable topic segment:

```bash
jmestrap --mqtt localhost --mqtt-sub "sensors/#"
```

Default mapping (`source_segment=1`):

| MQTT topic | segment 0 (category) | segment 1 (source) |
|---|---|---|
| `iothub/station_north` | `iothub` (ignored) | `station_north` |
| `can0/station_north` | `can0` (ignored) | `station_north` |
| `sensors/node_2/temp` | `sensors` (ignored) | `node_2` |

Segment 0 is the *category* (e.g. `iothub`, `can0`) — used for
merging streams from different transports at the broker level.
JMESTrap ignores it. Segment 2+ can be used by external tooling
(e.g. MQTT Explorer) but are not used by JMESTrap.

If the segment index is out of range, the source falls back to
`"unknown"`.

### SSE

Optional (`--features sse`). Connects to an HTTP SSE endpoint and
extracts the source from a JSON field:

```bash
jmestrap \
  --sse http://bridge:5000/stream \
  --sse-source-field sourceId
```

SSE is included as an illustrative second transport. It has no reconnect
behavior after stream termination.

### Mixed

Multiple ingress types can run simultaneously. Events from different
transports merge into the same source shards:

```bash
jmestrap \
  --mqtt localhost --mqtt-sub "sensors/#" \
  --sse http://bridge:5000/stream --sse-source-field sourceId
```

## Browser Demos

Two self-contained HTML files work against a running JMESTrap instance:

- **demo.html** — Interactive weather station demo. Injects simulated
  events and lets you create recordings with JMESPath predicates.
  Runs on anything with a browser — including a phone with JMESTrap
  in Termux.
- **dashboard.html** — Live observability dashboard showing sources
  and recordings with timing, predicate progress, and selectivity stats.
  Includes a *Predicate Painter*: click any value in a captured event
  to build a JMESPath predicate (AND/OR-composed) without typing.

Open either directly in a browser while JMESTrap is running on
`localhost:9000`, or visit `http://localhost:9000/ui` — the dashboard
is also served by the running binary.

## Client Libraries

The REST API is simple enough to drive from any HTTP client directly.
Two reference implementations are included as starting points:

- **Rust** (`client/rust/`) — async client with builder pattern (reqwest)
- **Python** (`client/python/`) — requests-based, with `assert_completed()` for pytest

A std-only Rust example (no reqwest) is also available:
`cargo run --example minimal_rust_client`

## Testing

```bash
cargo test                             # unit + integration tests
cargo test --features sse,mqtt         # with all transports
cargo test --test blackbox_binary      # spawned binary over real HTTP
```

Optional MQTT integration (requires a broker):

```bash
docker run -d --rm --name jmestrap-mosquitto -p 1883:1883 eclipse-mosquitto:2
JMESTRAP_RUN_MQTT_INTEGRATION=1 cargo test --features mqtt --test mqtt_integration
docker stop jmestrap-mosquitto
```

Optional stress/performance test:

```bash
JMESTRAP_RUN_PERF_GUARD=1 cargo test perf_guard_single_source_bounded
```

## Deployment

For long-running lab-rig deployment (systemd unit, auto-restart, journald
logs), see [`contrib/README.md`](contrib/README.md).

## Current Limitations

- Recording references are in-memory counters that reset on server restart.
  Do not persist refs across restarts.
- MQTT broker integration tests are opt-in to keep default workflows simple.
- SSE ingress has no reconnect behavior after stream termination.

## License

MIT
