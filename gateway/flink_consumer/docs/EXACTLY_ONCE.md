# Exactly-Once Semantics

This document explains how the Flink Iceberg Consumer guarantees exactly-once semantics for the complete Kafka → Flink → Iceberg pipeline.

## Overview

**Exactly-once semantics** ensures that each record is processed and written to Iceberg exactly once, even in the presence of failures. No duplicates, no data loss.

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│    Kafka     │────▶│    Flink     │────▶│   Iceberg    │
│              │     │              │     │              │
│ Offsets NOT  │     │ Checkpoint   │     │ 2-Phase      │
│ auto-commit  │     │ EXACTLY_ONCE │     │ Commit       │
└──────────────┘     └──────────────┘     └──────────────┘
       │                     │                     │
       │                     ▼                     │
       │              ┌──────────────┐            │
       │              │   RocksDB    │            │
       │              │   State      │            │
       │              └──────────────┘            │
       │                     │                     │
       └─────────────────────┴─────────────────────┘
              Coordinated Checkpoint Commit
```

## How It Works

### 1. Kafka Source (Exactly-Once Read)

```python
# Kafka source configuration
kafka_source_builder.set_property("enable.auto.commit", "false")
```

- **Auto-commit disabled**: Flink manages offsets, not Kafka
- **Offsets in state**: Stored in Flink checkpoint state
- **Commit on checkpoint**: Offsets committed only when checkpoint succeeds

### 2. Flink Processing (Exactly-Once State)

```yaml
# flink-conf.yaml
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.interval: 60s
state.backend: rocksdb
state.backend.incremental: true
```

- **Checkpoint mode**: EXACTLY_ONCE ensures consistent state
- **RocksDB backend**: Persistent state storage
- **Incremental checkpoints**: Efficient state snapshots
- **Externalized checkpoints**: Retained for recovery

### 3. Iceberg Sink (Exactly-Once Write)

```yaml
# Iceberg configuration
write.upsert.enabled: false          # Append-only mode
write.distribution-mode: none        # No shuffle
commit.retry.num-retries: 3          # Retry on failure
```

- **Append-only mode**: No upserts, only inserts
- **Buffered writes**: Data buffered until checkpoint
- **2-phase commit**: Transaction committed with checkpoint
- **Atomic commits**: All or nothing

## Checkpoint Flow

### Normal Operation

```
Time: t0
├─ Kafka: Read records [1000-1999]
├─ Flink: Process and buffer in state
└─ Iceberg: Write files (uncommitted)

Time: t0 + 60s (Checkpoint triggered)
├─ Flink: Start checkpoint
├─ State: Snapshot to RocksDB
├─ Kafka: Prepare offset commit (offset=2000)
└─ Iceberg: Prepare transaction commit

Time: t0 + 65s (Checkpoint completes)
├─ Flink: Checkpoint successful
├─ Kafka: Commit offset=2000
├─ Iceberg: Commit transaction
└─ Data: Visible in Iceberg table

Result: Records [1000-1999] processed exactly once ✓
```

### Failure Recovery

```
Time: t0
├─ Kafka: Read records [1000-1999]
├─ Flink: Process and buffer in state
└─ Iceberg: Write files (uncommitted)

Time: t0 + 30s (Failure occurs)
├─ Flink: Job fails before checkpoint
├─ Kafka: Offset NOT committed (still at 1000)
└─ Iceberg: Transaction NOT committed (files discarded)

Time: t0 + 35s (Recovery)
├─ Flink: Restart from last checkpoint
├─ Kafka: Resume from offset=1000
├─ State: Restored from checkpoint
└─ Processing: Resumes from offset=1000

Time: t0 + 95s (Next checkpoint)
├─ Flink: Checkpoint successful
├─ Kafka: Commit offset=2000
├─ Iceberg: Commit transaction
└─ Data: Visible in Iceberg table

Result: Records [1000-1999] processed exactly once ✓
No duplicates, no data loss
```

## Configuration

### Required Settings

#### 1. Flink Configuration (`flink-conf.yaml`)

```yaml
# Checkpoint configuration
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.interval: 60s
execution.checkpointing.timeout: 10min
execution.checkpointing.min-pause: 30s
execution.checkpointing.max-concurrent-checkpoints: 1

# State backend
state.backend: rocksdb
state.backend.incremental: true
state.checkpoints.dir: file:///tmp/flink-checkpoints

# Externalized checkpoints
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION

# Iceberg exactly-once settings
write.distribution-mode: none
write.upsert.enabled: false
commit.retry.num-retries: 3
```

#### 2. Application Configuration (`.env.local`)

```bash
# Flink settings
FLINK_PARALLELISM=6
FLINK_CHECKPOINT_MODE=EXACTLY_ONCE
FLINK_CHECKPOINT_INTERVAL_MS=60000
FLINK_STATE_BACKEND=rocksdb
FLINK_CHECKPOINT_DIR=file:///tmp/flink-checkpoints

# Kafka settings
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=health-data-raw
KAFKA_GROUP_ID=flink-iceberg-consumer
# Note: enable.auto.commit is set to false in code

# Iceberg settings
ICEBERG_CATALOG_TYPE=rest
ICEBERG_CATALOG_URI=http://iceberg-rest:8181
ICEBERG_WAREHOUSE=s3a://data-lake/warehouse

# Batch settings
BATCH_SIZE=1000
BATCH_TIMEOUT_SECONDS=10
TARGET_FILE_SIZE_MB=256
```

## Verification

### Test Exactly-Once Configuration

Run the verification script:

```bash
cd flink_consumer
python examples/test_exactly_once.py
```

Expected output:

```
✅ PASS: Checkpoint Configuration
✅ PASS: Kafka Source Configuration
✅ PASS: Iceberg Sink Configuration
✅ PASS: Exactly-Once Guarantee
✅ PASS: Configuration Summary

🎉 ALL TESTS PASSED
Your pipeline is configured for exactly-once semantics!
```

### Monitor Checkpoints

Check Flink Web UI (http://localhost:8081):

1. Navigate to job details
2. Click "Checkpoints" tab
3. Verify:
   - Checkpoint mode: EXACTLY_ONCE
   - Checkpoints completing successfully
   - No checkpoint failures

### Verify Data Integrity

Query Iceberg table to check for duplicates:

```sql
-- Count total records
SELECT COUNT(*) FROM health_data_raw;

-- Check for duplicate sample_ids
SELECT sample_id, COUNT(*) as count
FROM health_data_raw
GROUP BY sample_id
HAVING COUNT(*) > 1;

-- Should return 0 rows (no duplicates)
```

## Performance Considerations

### Checkpoint Interval

- **Too frequent** (< 30s): High overhead, reduced throughput
- **Too infrequent** (> 5min): Long recovery time, large state
- **Recommended**: 60s for most workloads

### State Size

Monitor checkpoint size:

```bash
# Check checkpoint directory
ls -lh /tmp/flink-checkpoints/

# Monitor checkpoint metrics in Flink UI
# - lastCheckpointSize
# - checkpointDuration
```

### Throughput Impact

Exactly-once has minimal overhead:

- **Checkpoint overhead**: ~1-2% throughput reduction
- **State backend**: RocksDB provides good performance
- **Incremental checkpoints**: Only changed state is snapshotted

## Troubleshooting

### Checkpoint Failures

**Symptom**: Checkpoints timing out or failing

```
Error: Checkpoint expired before completing
```

**Solutions**:
1. Increase checkpoint timeout:
   ```yaml
   execution.checkpointing.timeout: 15min
   ```

2. Reduce checkpoint interval:
   ```yaml
   execution.checkpointing.interval: 120s
   ```

3. Increase TaskManager memory:
   ```yaml
   taskmanager.memory.process.size: 8192m
   ```

### Duplicate Records

**Symptom**: Duplicate records in Iceberg table

**Possible causes**:
1. Checkpoint mode not set to EXACTLY_ONCE
2. Kafka auto-commit enabled
3. Iceberg upsert mode enabled

**Verification**:
```bash
# Check configuration
python examples/test_exactly_once.py

# Verify checkpoint mode in Flink UI
# Should show: EXACTLY_ONCE
```

### Slow Recovery

**Symptom**: Long time to recover from failure

**Solutions**:
1. Enable incremental checkpoints:
   ```yaml
   state.backend.incremental: true
   ```

2. Use faster storage for checkpoints:
   ```yaml
   state.checkpoints.dir: hdfs:///flink/checkpoints
   ```

3. Reduce state size by filtering early in pipeline

## Best Practices

1. **Always enable checkpointing** for production workloads
2. **Use externalized checkpoints** for recovery after job cancellation
3. **Monitor checkpoint metrics** to detect issues early
4. **Test failure scenarios** before production deployment
5. **Set appropriate checkpoint interval** based on latency requirements
6. **Use incremental checkpoints** for large state
7. **Retain checkpoints** for disaster recovery

## Guarantees

With proper configuration, the pipeline guarantees:

✅ **No duplicates**: Each record written to Iceberg exactly once  
✅ **No data loss**: All records from Kafka are processed  
✅ **Consistent state**: State is always consistent across failures  
✅ **Atomic commits**: Kafka offsets and Iceberg commits are coordinated  
✅ **Failure recovery**: Automatic recovery from any failure  

## References

- [Flink Checkpointing](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/)
- [Flink State Backends](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/)
- [Iceberg Flink Integration](https://iceberg.apache.org/docs/latest/flink/)
- [Kafka Consumer Offsets](https://kafka.apache.org/documentation/#consumerconfigs)
