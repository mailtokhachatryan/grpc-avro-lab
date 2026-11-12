#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
host=127.0.0.1
port=8080
server_pid=
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Port $port is occupied; stop/reconfigure its owner before this smoke check." >&2
  exit 1
fi
./gradlew -q :grpc-server:installDist :grpc-client-java:installDist
mkdir -p build/smoke/grpc
GRPC_HOST="$host" GRPC_PORT="$port" grpc-server/build/install/grpc-server/bin/grpc-server >build/smoke/grpc/server.log 2>&1 &
server_pid=$!
ready_deadline=$((SECONDS + 30))
until lsof -a -p "$server_pid" -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; do
  if ! kill -0 "$server_pid" 2>/dev/null || (( SECONDS >= ready_deadline )); then
    cat build/smoke/grpc/server.log >&2
    exit 1
  fi
  sleep 0.2
done

client=(grpc-client-java/build/install/grpc-client-java/bin/grpc-client-java --host "$host" --port "$port")
response=$(GRPC_PORT=invalid "${client[@]}")
[[ "$response" == Pong ]]
echo "PASS: Java client → $host:$port, Ping → $response"
if "${client[@]}" --message=invalid >build/smoke/grpc/invalid.out 2>build/smoke/grpc/invalid.err; then
  echo "Invalid input unexpectedly succeeded" >&2
  exit 1
fi
grep -Fq INVALID_ARGUMENT build/smoke/grpc/invalid.err
echo "PASS: invalid input → INVALID_ARGUMENT (nonzero exit)"

cleanup
server_pid=
if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Server did not release port $port" >&2
  exit 1
fi
if "${client[@]}" --deadline-ms 500 >build/smoke/grpc/unavailable.out 2>build/smoke/grpc/unavailable.err; then
  echo "Unavailable server unexpectedly succeeded" >&2
  exit 1
fi
grep -Eq 'UNAVAILABLE|DEADLINE_EXCEEDED' build/smoke/grpc/unavailable.err
echo "PASS: server shutdown releases 8080; unavailable endpoint fails within bounded call"
