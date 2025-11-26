# Kafka Configuration

Kafka cluster configuration and management scripts using KRaft mode (no ZooKeeper).

## Cluster Setup

The cluster consists of:
- **3 Kafka Brokers** in KRaft mode (broker-1, broker-2, broker-3)
  - Each broker acts as both broker and controller
  - Controller quorum for metadata management
- **Schema Registry** for Avro schema management
- **Kafka UI** for monitoring and management

## KRaft Mode

This cluster uses Kafka's KRaft (Kafka Raft) mode instead of ZooKeeper:
- No ZooKeeper dependency
- Built-in metadata management via Raft consensus
- Faster metadata operations
- Simplified architecture

## Topics

### health-data-raw
- **Purpose**: Main topic for health data from iOS app
- **Partitions**: 6
- **Replication Factor**: 3
- **Retention**: 7 days (604800000 ms)
- **Min In-Sync Replicas**: 2

### health-data-dlq
- **Purpose**: Dead Letter Queue for failed messages
- **Partitions**: 3
- **Replication Factor**: 3
- **Retention**: 30 days (2592000000 ms)
- **Min In-Sync Replicas**: 2

## Starting the Cluster

Start all services:
```bash
docker-compose up -d
```

Check service status:
```bash
docker-compose ps
```

View logs:
```bash
docker-compose logs -f kafka-broker-1
```

## Initialize Topics

Manually create topics:
```bash
docker-compose exec kafka-broker-1 kafka-topics \
  --create \
  --bootstrap-server kafka-broker-1:9092,kafka-broker-2:9094,kafka-broker-3:9096 \
  --topic health-data-raw \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config min.insync.replicas=2

docker-compose exec kafka-broker-1 kafka-topics \
  --create \
  --bootstrap-server kafka-broker-1:9092,kafka-broker-2:9094,kafka-broker-3:9096 \
  --topic health-data-dlq \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=2592000000 \
  --config min.insync.replicas=2
```

## Kafka UI

Access the Kafka UI at: http://localhost:8080

Features:
- View topics, partitions, and messages
- Monitor consumer groups
- View schema registry schemas
- Cluster health monitoring

## Schema Registry

Access Schema Registry at: http://localhost:8081

List schemas:
```bash
curl http://localhost:8081/subjects
```

Get schema details:
```bash
curl http://localhost:8081/subjects/health-data-raw-value/versions/latest
```

## Useful Commands

### List Topics
```bash
docker-compose exec kafka-broker-1 kafka-topics \
  --list \
  --bootstrap-server kafka-broker-1:9092
```

### Describe Topic
```bash
docker-compose exec kafka-broker-1 kafka-topics \
  --describe \
  --topic health-data-raw \
  --bootstrap-server kafka-broker-1:9092
```

### Consume Messages
```bash
docker-compose exec kafka-broker-1 kafka-console-consumer \
  --bootstrap-server kafka-broker-1:9092 \
  --topic health-data-raw \
  --from-beginning
```

### Produce Test Message
```bash
docker-compose exec kafka-broker-1 kafka-console-producer \
  --bootstrap-server kafka-broker-1:9092 \
  --topic health-data-raw
```

### Check Consumer Groups
```bash
docker-compose exec kafka-broker-1 kafka-consumer-groups \
  --list \
  --bootstrap-server kafka-broker-1:9092
```

## Stopping the Cluster

Stop all services:
```bash
docker-compose down
```

Stop and remove volumes (data will be lost):
```bash
docker-compose down -v
```

## Troubleshooting

### Broker not starting
Check logs:
```bash
docker-compose logs kafka-broker-1
```

### ZooKeeper connection issues
Verify ZooKeeper is running:
```bash
docker-compose ps zookeeper
```

### Topic creation fails
Ensure all brokers are running:
```bash
docker-compose ps | grep kafka-broker
```

## Configuration Files

- `docker-compose.yml`: Main cluster configuration
- `init-topics.sh`: Topic initialization script
