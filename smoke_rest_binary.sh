#!/bin/bash
#
# Binary-level REST smoke test for JMESTrap.
#
# Builds and starts jmestrap, then validates one black-box happy path.

set -euo pipefail

BASE="http://127.0.0.1:9000"
PORT=9000
LOG_FILE="/tmp/jmestrap_rest_smoke.log"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

SERVER_PID=""

cleanup() {
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}

trap cleanup EXIT

request() {
  local method="$1"
  local path="$2"
  local data="${3:-}"
  local tmp
  tmp="$(mktemp)"

  if [ -n "$data" ]; then
    STATUS="$(curl -sS -o "$tmp" -w "%{http_code}" -X "$method" "$BASE$path" -H 'Content-Type: application/json' -d "$data")"
  else
    STATUS="$(curl -sS -o "$tmp" -w "%{http_code}" -X "$method" "$BASE$path")"
  fi

  BODY="$(<"$tmp")"
  rm -f "$tmp"
}

assert_status() {
  local expected="$1"
  if [ "$STATUS" != "$expected" ]; then
    echo -e "${RED}Expected HTTP $expected, got $STATUS${NC}"
    echo "$BODY"
    exit 1
  fi
}

assert_body_has() {
  local pattern="$1"
  if ! printf '%s' "$BODY" | grep -Eq "$pattern"; then
    echo -e "${RED}Response did not match: $pattern${NC}"
    echo "$BODY"
    exit 1
  fi
}

extract_reference() {
  printf '%s' "$BODY" | sed -n 's/.*"reference"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n 1
}

echo -e "${YELLOW}=== JMESTrap REST Smoke Test ===${NC}"

echo -e "${YELLOW}Building release binary...${NC}"
cargo build --release --quiet

echo -e "${YELLOW}Starting jmestrap on :$PORT...${NC}"
./target/release/jmestrap >"$LOG_FILE" 2>&1 &
SERVER_PID="$!"

for _ in $(seq 1 50); do
  if curl -fsS "$BASE/ping" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

request GET /ping
assert_status 200
assert_body_has '"status"[[:space:]]*:[[:space:]]*"ok"'
echo -e "${GREEN}✓ ping${NC}"

request POST /recordings "{\"sources\":[\"smoke_sensor\"],\"until\":{\"type\":\"order\",\"predicates\":[\"event=='start'\",\"event=='done'\"]}}"
assert_status 201
REF1="$(extract_reference)"
if [ -z "$REF1" ]; then
  echo -e "${RED}Failed to parse recording reference${NC}"
  echo "$BODY"
  exit 1
fi

request POST /events/smoke_sensor '{"event":"start","value":1}'
assert_status 202
request POST /events/smoke_sensor '{"event":"noise","value":2}'
assert_status 202
request POST /events/smoke_sensor '{"event":"done","value":3}'
assert_status 202

request GET "/recordings/$REF1?timeout=3"
assert_status 200
assert_body_has '"finished"[[:space:]]*:[[:space:]]*true'
echo -e "${GREEN}✓ recording completes through binary + HTTP path${NC}"

request DELETE "/recordings/$REF1"
assert_status 200
request GET "/recordings/$REF1"
assert_status 404
echo -e "${GREEN}✓ recording lifecycle${NC}"

request GET /sources
assert_status 200
assert_body_has '"source"[[:space:]]*:[[:space:]]*"smoke_sensor"'
echo -e "${GREEN}✓ sources endpoint tracks observed source${NC}"

echo -e "${GREEN}=== REST smoke test passed ===${NC}"
