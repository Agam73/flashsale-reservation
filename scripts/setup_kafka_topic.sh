#!/usr/bin/env bash
# Creates (or recreates) the checkout-attempts topic with an explicit
# partition count. Run this before starting checkout-api or
# decision-service for the first time, and again any time you want a
# clean slate (e.g. to clear out Phase 5's manual test data or fix a
# topic that got auto-created with the wrong partition count).
#
# Usage: scripts/setup_kafka_topic.sh [partitions]
#   scripts/setup_kafka_topic.sh        # defaults to 3 partitions
#   scripts/setup_kafka_topic.sh 6      # 6 partitions instead

set -euo pipefail

TOPIC="checkout-attempts"
PARTITIONS="${1:-3}"
CONTAINER="flashsale-kafka"
BOOTSTRAP="localhost:9092"

echo "Checking for an existing '$TOPIC' topic..."
if docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" --list | grep -qx "$TOPIC"; then
  echo "Found an existing '$TOPIC' topic -- deleting it first so there's no leftover"
  echo "test data or a mismatched partition count from an earlier auto-create."
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC"
  sleep 2
fi

echo "Creating '$TOPIC' with $PARTITIONS partitions..."
docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1

echo ""
echo "Done. Current topic config:"
docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC"
