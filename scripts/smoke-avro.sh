#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Use a fresh topic and groups so previous runs cannot change the results.
export KAFKA_TOPIC="lab.smoke.$(date +%s).$$"
export KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:${LAB_KAFKA_PORT:-9092}"
export SCHEMA_REGISTRY_URL="http://127.0.0.1:${LAB_REGISTRY_PORT:-8081}"
registry=$SCHEMA_REGISTRY_URL
subject="$KAFKA_TOPIC-value"
./scripts/bootstrap-local.sh
./gradlew -q :avro-messaging:installDist
application=avro-messaging/build/install/avro-messaging/bin/avro-messaging
evidence_dir=build/smoke/avro
mkdir -p "$evidence_dir"
consumer_pid=

cleanup() {
  if [[ -n "$consumer_pid" ]]; then
    kill "$consumer_pid" 2>/dev/null || true
    wait "$consumer_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

"$application" consume --reader-schema v2 --count 2 --group "$KAFKA_TOPIC.v2" --timeout 45s >"$evidence_dir/reader-v2.log" &
consumer_pid=$!
"$application" produce --schema v1 --event-id first --message Hello-v1 | tee "$evidence_dir/writer-v1.log"
"$application" produce --schema v2 --event-id second --message Hello-v2 --source java-producer | tee "$evidence_dir/writer-v2.log"
wait "$consumer_pid"
consumer_pid=
cat "$evidence_dir/reader-v2.log"
grep -Eq '"event_id"[[:space:]]*:[[:space:]]*"first".*"source"[[:space:]]*:[[:space:]]*null' "$evidence_dir/reader-v2.log"
grep -Eq '"event_id"[[:space:]]*:[[:space:]]*"second".*"source"[[:space:]]*:[[:space:]]*"java-producer"' "$evidence_dir/reader-v2.log"

"$application" consume --reader-schema v1 --count 2 --group "$KAFKA_TOPIC.v1" | tee "$evidence_dir/reader-v1.log"
grep -Fq 'Hello-v1' "$evidence_dir/reader-v1.log"
grep -Fq 'Hello-v2' "$evidence_dir/reader-v1.log"
if grep -Fq '"source"' "$evidence_dir/reader-v1.log"; then
  echo "V1 reader unexpectedly exposed the V2 field" >&2
  exit 1
fi

"$application" inspect --count 2 | tee "$evidence_dir/inspection.log"
for version in 1 2; do
  registry_id=$(curl --fail --silent --show-error --max-time 10 "$registry/subjects/$subject/versions/$version" | jq -er '.id')
  printf -v prefix '00 %02x %02x %02x %02x' "$((registry_id >> 24 & 255))" "$((registry_id >> 16 & 255))" "$((registry_id >> 8 & 255))" "$((registry_id & 255))"
  grep -Fq "schemaId=$registry_id subjectVersion=$version prefix=$prefix" "$evidence_dir/inspection.log"
done

invalid=$(jq -cRs '{schemaType:"AVRO", schema:.}' contracts/avro/incompatible/message-event.avsc)
content_type='Content-Type: application/vnd.schemaregistry.v1+json'
curl --fail --silent --show-error --max-time 10 --header "$content_type" --data "$invalid" \
  "$registry/compatibility/subjects/$subject/versions/latest" | jq -e '.is_compatible == false' >/dev/null
status=$(curl --silent --show-error --max-time 10 --output "$evidence_dir/rejected.json" --write-out '%{http_code}' \
  --header "$content_type" --data "$invalid" "$registry/subjects/$subject/versions")
[[ $status == 409 ]]
curl --fail --silent --show-error --max-time 10 "$registry/subjects/$subject/versions" | jq -e '. == [1,2]' >/dev/null
echo "PASS: V1/V2 reader projection; embedded IDs match Registry; incompatible registration rejected (HTTP 409)"
echo "Demo topic retained: $KAFKA_TOPIC"
