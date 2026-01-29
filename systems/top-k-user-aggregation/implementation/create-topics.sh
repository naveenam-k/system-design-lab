#!/usr/bin/env bash
set -euo pipefail

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:29092}"

create_topic() {
  local name="$1"
  local partitions="${2:-6}"
  local replicas="${3:-1}"
  docker run --rm --network host confluentinc/cp-kafka:7.6.1 \
    kafka-topics \
      --bootstrap-server "$KAFKA_BOOTSTRAP" \
      --create --if-not-exists \
      --topic "$name" \
      --partitions "$partitions" \
      --replication-factor "$replicas"
}

# crawl.jobs removed — using Asynq (Redis) for job scheduling instead
create_topic "user.listen.raw" 12 1

echo "Topics created."
