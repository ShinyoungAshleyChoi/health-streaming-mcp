# Checkpoint and Recovery Configuration

This document describes the checkpoint and state management configuration for the Flink Iceberg Consumer application.

## Overview

The checkpoint and recovery system ensures exactly-once semantics and fault tolerance for the health data streaming pipeline. It consists of two main components:

1. **Checkpoint Configuration**: Manages state snapshots and exactly-once guarantees
2. **Recovery Strategy**: Handles failure recovery and restart policies

## Requirements

- **5.1**: Checkpoint state snapshots at regular intervals
- **5.2**: Store state in RocksDB backend with S3 checkpoint storage
- **5.3**: Exactly-once semantics with externalized checkpoints
- **5.4**: Fixed delay restart strategy for failure recovery
- **5.5**: Checkpoint failure handling and tolerance

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Flink Application                         │
│                                                              │
│  ┌────────────────┐      ┌──────────────────┐              │
│  │  Kafka Source  │─────▶│  Transformation  │              │
│  └────────────────┘      └──────────────────┘              │
│          │                        │                          │
│          │                        │                          │
│          ▼                        ▼                          │
│  ┌────────────────────────────────────────┐                 │
│  │      Checkpoint Coordinator            │                 │
│  │  - Trigger checkpoints every 60s       │                 │
│  │  - Coordinate state snapshots          │                 │
│  │  - Commit Kafka offsets                │                 │
│  └────────────────────────────────────────┘                 │
│          │                                                   │
└──────────┼───────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│                  State Backend (RocksDB)                     │
│  - Stores operator state                                     │
│  - Supports incremental checkpoints                          │
│  - Handles large state efficiently                           │
└─────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│            Checkpoint Storage (S3/MinIO)                     │
│  - s3a://flink-checkpoints/health-consumer                   │
│  - Externalized checkpoints (retained on cancellation)       │
│  - Enables recovery from failures                            │
└─────────────────────────────────────────────────────────────┘
```

## Configuration

### Environment Variables

Add the following to your `.env.local` or `.env` file:

```bash
# Checkpoint Configuration
FLINK_CHECKPOINT_INTERVAL_MS=60000              # 60 seconds
FLINK_CHECKPOINT_TIMEOUT_MS=600000              # 10 minutes
FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS_MS=30000    # 30 seconds
FLINK_MAX_CONCURRENT_CHECKPOINTS=1
FLINK_CHECKPOINT_MODE=EXACTLY_ONCE
FLINK_STATE_BACKEND=rocksdb
FLINK_CHECKPOINT_STORAGE=s3a://flink-checkpoints/health-consumer

# Restart Strategy Configuration
FLINK_RESTART_STRATEGY=fixed-delay
FLINK_RESTART_ATTEMPTS=3
FLINK_RESTART_DELAY_MS=10000                    # 10 seconds

# Failure Rate Strategy (optional)
FLINK_FAILURE_RATE_MAX_FAILURES=3
FLINK_FAILURE_RATE_INTERVAL_MS=300000           # 5 minutes
FLINK_FAILURE_RATE_DELAY_MS=10000               # 10 seconds
```

### S3 Configuration for Checkpoints

Add the following to your `flink-conf.yaml` or set as environment variables:

```yaml
# S3/MinIO Configuration
fs.s3a.endpoint: http://minio:9000
fs.s3a.access.key: minioadmin
fs.s3a.secret.key: minioadmin
fs.s3a.path.style.access: true
fs.s3a.connection.ssl.enabled: false
```

## Usage

### Quick Start (Recommended)

Use the convenience function to set up both checkpoint and recovery:

```python
from pyflink.datastream import StreamExecutionEnvironment
from flink_consumer.config import setup_checkpoint_and_recovery, settings

# Create execution environment
env = StreamExecutionEnvironment.get_execution_environment()

# Set parallelism
env.set_parallelism(settings.flink.parallelism)

# Configure checkpoint and recovery
setup_checkpoint_and_recovery(env, settings.flink)

# Continue with your pipeline...
```

### Checkpoint Only

Configure only checkpoint and state management:

```python
from flink_consumer.config import setup_checkpoint_and_state, settings

setup_checkpoint_and_state(env, settings.flink)
```

### Recovery Only

Configure only restart strategy:

```python
from flink_consumer.config import setup_recovery_strategy, settings

setup_recovery_strategy(env, settings.flink)
```

### Custom Configuration

For fine-grained control:

```python
from flink_consumer.config.checkpoint import CheckpointConfig
from flink_consumer.config.recovery import RecoveryConfig
from flink_consumer.config import settings

# Create configuration objects
checkpoint_config = CheckpointConfig(settings.flink)
recovery_config = RecoveryConfig(settings.flink)

# Apply configurations step by step
checkpoint_config.configure_state_backend(env)
checkpoint_config.configure_checkpoint(env)
recovery_config.configure_restart_strategy(env)
recovery_config.configure_checkpoint_failure_handling(env)
```

## Checkpoint Behavior

### Checkpoint Interval

Checkpoints are triggered every 60 seconds (configurable via `FLINK_CHECKPOINT_INTERVAL_MS`).

### Exactly-Once Semantics

The system guarantees exactly-once processing:

1. **Checkpoint Barrier**: Coordinator injects checkpoint barriers into the stream
2. **State Snapshot**: Each operator creates a state snapshot
3. **Kafka Offset Commit**: Kafka offsets are committed only after successful checkpoint
4. **Iceberg Commit**: Iceberg writes are committed atomically with checkpoint

### Checkpoint Timeout

If a checkpoint takes longer than 10 minutes (configurable via `FLINK_CHECKPOINT_TIMEOUT_MS`), it will be aborted.

### Externalized Checkpoints

Checkpoints are retained even after job cancellation, enabling:
- Manual recovery from specific checkpoint
- Job upgrades with state preservation
- Disaster recovery

## Recovery Behavior

### Restart Strategies

#### Fixed Delay (Recommended for Production)

Restarts the job up to 3 times with 10-second delays between attempts:

```bash
FLINK_RESTART_STRATEGY=fixed-delay
FLINK_RESTART_ATTEMPTS=3
FLINK_RESTART_DELAY_MS=10000
```

**Behavior:**
- Attempt 1: Immediate restart
- Attempt 2: Wait 10s, then restart
- Attempt 3: Wait 10s, then restart
- After 3 failures: Job fails permanently

#### Failure Rate

Restarts if failure rate doesn't exceed threshold:

```bash
FLINK_RESTART_STRATEGY=failure-rate
FLINK_FAILURE_RATE_MAX_FAILURES=3
FLINK_FAILURE_RATE_INTERVAL_MS=300000  # 5 minutes
FLINK_FAILURE_RATE_DELAY_MS=10000
```

**Behavior:**
- Allows up to 3 failures within 5-minute window
- If exceeded, job fails permanently
- Useful for handling transient failures

#### No Restart (Development Only)

Job will not restart on failure:

```bash
FLINK_RESTART_STRATEGY=none
```

**Warning:** Not recommended for production!

### Failover Strategy

The system uses **region failover** by default:
- Only affected tasks are restarted
- Minimizes recovery time
- Reduces resource usage

To change to full failover, set in `flink-conf.yaml`:

```yaml
jobmanager.execution.failover-strategy: full
```

### Checkpoint Failure Handling

The system tolerates up to 3 consecutive checkpoint failures before failing the job. This prevents job failure due to transient checkpoint issues.

## Recovery Scenarios

### Scenario 1: TaskManager Failure

1. TaskManager crashes
2. JobManager detects failure
3. Restart strategy triggers (fixed delay)
4. Job restarts from last successful checkpoint
5. Kafka offsets are reset to checkpoint position
6. Processing resumes

**Recovery Time:** ~10-30 seconds (depending on state size)

### Scenario 2: Checkpoint Failure

1. Checkpoint attempt fails (e.g., S3 unavailable)
2. System tolerates failure (up to 3 consecutive failures)
3. Next checkpoint attempt proceeds normally
4. If 4th consecutive failure occurs, job fails

**Action:** Monitor checkpoint success rate

### Scenario 3: Job Upgrade

1. Stop job with savepoint:
   ```bash
   flink stop <job-id> --savepointPath s3a://flink-checkpoints/savepoints
   ```

2. Deploy new version

3. Start job from savepoint:
   ```bash
   flink run --fromSavepoint s3a://flink-checkpoints/savepoints/<savepoint-id> ...
   ```

### Scenario 4: Data Corruption Recovery

1. Identify corrupted data time range
2. Stop job
3. Rollback Iceberg table to snapshot before corruption
4. Restart job from checkpoint before corruption
5. Reprocess data

## Monitoring

### Key Metrics

Monitor these metrics via Prometheus/Grafana:

- `flink_jobmanager_job_lastCheckpointDuration`: Checkpoint duration
- `flink_jobmanager_job_lastCheckpointSize`: Checkpoint size
- `flink_jobmanager_job_numberOfCompletedCheckpoints`: Successful checkpoints
- `flink_jobmanager_job_numberOfFailedCheckpoints`: Failed checkpoints
- `flink_jobmanager_job_numRestarts`: Number of job restarts

### Alerts

Set up alerts for:

1. **High Checkpoint Duration**
   ```yaml
   alert: HighCheckpointDuration
   expr: flink_jobmanager_job_lastCheckpointDuration > 300000  # 5 minutes
   ```

2. **Checkpoint Failures**
   ```yaml
   alert: CheckpointFailures
   expr: rate(flink_jobmanager_job_numberOfFailedCheckpoints[5m]) > 0.1
   ```

3. **Frequent Restarts**
   ```yaml
   alert: FrequentRestarts
   expr: rate(flink_jobmanager_job_numRestarts[10m]) > 0.5
   ```

## Best Practices

### Production Configuration

```bash
# Checkpoint every 60 seconds
FLINK_CHECKPOINT_INTERVAL_MS=60000

# Use RocksDB for large state
FLINK_STATE_BACKEND=rocksdb

# Exactly-once semantics
FLINK_CHECKPOINT_MODE=EXACTLY_ONCE

# Fixed delay restart (3 attempts, 10s delay)
FLINK_RESTART_STRATEGY=fixed-delay
FLINK_RESTART_ATTEMPTS=3
FLINK_RESTART_DELAY_MS=10000
```

### Development Configuration

```bash
# More frequent checkpoints for faster testing
FLINK_CHECKPOINT_INTERVAL_MS=30000

# HashMapStateBackend for faster development
FLINK_STATE_BACKEND=hashmap

# No restart for easier debugging
FLINK_RESTART_STRATEGY=none
```

### State Size Optimization

1. **Use RocksDB for large state** (> 1GB)
2. **Enable incremental checkpoints** (automatic with RocksDB)
3. **Set state TTL** for windowed operations
4. **Monitor state size growth**

### Checkpoint Storage

1. **Use S3/MinIO** for durability
2. **Enable versioning** on S3 bucket
3. **Set retention policy** (e.g., 7 days)
4. **Monitor storage costs**

## Troubleshooting

### Checkpoint Timeout

**Symptom:** Checkpoints consistently timeout

**Causes:**
- State too large
- Slow S3 writes
- Backpressure in pipeline

**Solutions:**
- Increase `FLINK_CHECKPOINT_TIMEOUT_MS`
- Optimize state size
- Increase parallelism
- Check S3 performance

### Frequent Restarts

**Symptom:** Job restarts repeatedly

**Causes:**
- Kafka connection issues
- Iceberg write failures
- Resource exhaustion

**Solutions:**
- Check Kafka connectivity
- Verify Iceberg catalog access
- Increase TaskManager resources
- Review error logs

### State Size Growth

**Symptom:** Checkpoint size increases over time

**Causes:**
- Unbounded state accumulation
- Missing state TTL
- Memory leaks

**Solutions:**
- Set state TTL for windowed operations
- Review state usage patterns
- Enable state cleanup

## Testing

Run the example script to test configuration:

```bash
cd flink_consumer
python examples/test_checkpoint_recovery.py
```

This will demonstrate:
1. Checkpoint configuration
2. Recovery strategy configuration
3. Complete configuration
4. Custom configuration
5. Configuration summary

## References

- [Apache Flink Checkpointing](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/)
- [Flink State Backends](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/)
- [Flink Restart Strategies](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/task_failure_recovery/)
- [PyFlink Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/overview/)

## Summary

The checkpoint and recovery system provides:

✅ **Exactly-once semantics** for data processing  
✅ **Fault tolerance** with automatic recovery  
✅ **State persistence** in RocksDB and S3  
✅ **Flexible restart strategies** for different scenarios  
✅ **Production-ready configuration** with monitoring  

For questions or issues, refer to the troubleshooting section or check the Flink logs.
