#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

topic=${KAFKA_TOPIC:-lab.messages}
registry="http://127.0.0.1:${LAB_REGISTRY_PORT:-8081}"
if [[ ! $topic =~ ^[a-zA-Z0-9._-]{1,249}$ || $topic == . || $topic == .. ]]; then
  echo "Invalid Kafka topic name" >&2
  exit 2
fi

compose=(docker compose -f infra/compose.yaml)
"${compose[@]}" up -d --wait --wait-timeout 150
kafka_topics=("${compose[@]}" exec -T kafka kafka-topics --bootstrap-server kafka:29092)
"${kafka_topics[@]}" --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1
description=$("${kafka_topics[@]}" --describe --topic "$topic")
printf '%s\n' "$description"
if ! grep -Eq 'PartitionCount:[[:space:]]+1[[:space:]]+ReplicationFactor:[[:space:]]+1([[:space:]]|$)' <<< "$description"; then
  echo "Existing topic $topic must have one partition and replication factor one" >&2
  exit 1
fi
curl --fail --silent --show-error --max-time 10 --request PUT \
  --header 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data '{"compatibility":"BACKWARD_TRANSITIVE"}' "${registry%/}/config/$topic-value"
printf '\n'
