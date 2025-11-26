#!/bin/bash
# Kafka topic initialization script for KRaft mode

set -e

echo "Waiting for Kafka brokers to be ready..."
sleep 15

KAFKA_BROKERS="kafka-broker-1:9092,kafka-broker-2:9094,kafka-broker-3:9096"

echo "Creating health-data-raw topic..."
kafka-topics --create \
  --bootstrap-server $KAFKA_BROKERS \
  --topic health-data-raw \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config min.insync.replicas=2 \
  --if-not-exists

echo "Creating health-data-dlq topic..."
kafka-topics --create \
  --bootstrap-server $KAFKA_BROKERS \
  --topic health-data-dlq \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=2592000000 \
  --config min.insync.replicas=2 \
  --if-not-exists

echo "Listing all topics..."
kafka-topics --list --bootstrap-server $KAFKA_BROKERS

echo "Describing topics..."
kafka-topics --describe --bootstrap-server $KAFKA_BROKERS

echo "Topic initialization complete!"
