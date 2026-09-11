#!/usr/bin/env bash
# Creates (or recreates) the checkout-attempts topic with an explicit
# partition count, plus the checkout-attempts-dlq topic (Phase 8) that
# decision-service publishes to when it gives up on a message. Run this
# before starting checkout-api or decision-service for the first time,
# and again any time you want a clean slate (e.g. to clear out Phase
# 5's manual test data or fix a topic that got auto-created with the
# wrong partition count).
#
# Usage: scripts/setup_kafka_topic.sh [partitions]
#   scripts/setup_kafka_topic.sh        # main topic defaults to 3 partitions
#   scripts/setup_kafka_topic.sh 6      # 6 partitions instead
#
# The DLQ topic always gets 1 partition -- it's an audit/inspection
# log, not a work queue, so there's no per-item ordering to preserve.

set -euo pipefail

MAIN_TOPIC="checkout-attempts"
DLQ_TOPIC="checkout-attempts-dlq"
MAIN_PARTITIONS="${1:-3}"
CONTAINER="flashsale-kafka"
BOOTSTRAP="localhost:9092"

create_topic() {
  local topic="$1"
  local partitions="$2"

  echo "Checking for an existing '$topic' topic..."
  if docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "$BOOTSTRAP" --list | grep -qx "$topic"; then
    echo "Found an existing '$topic' topic -- deleting it first so there's no leftover"
    echo "test data or a mismatched partition count from an earlier auto-create."
    docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "$BOOTSTRAP" --delete --topic "$topic"
    sleep 2
  fi

  echo "Creating '$topic' with $partitions partition(s)..."
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create --topic "$topic" --partitions "$partitions" --replication-factor 1
}

create_topic "$MAIN_TOPIC" "$MAIN_PARTITIONS"
create_topic "$DLQ_TOPIC" 1

echo ""
echo "Done. Current topic configs:"
docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --describe --topic "$MAIN_TOPIC"
docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --describe --topic "$DLQ_TOPIC"
