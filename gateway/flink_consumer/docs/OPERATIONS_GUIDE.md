# Operations Guide

This guide provides comprehensive instructions for deploying, monitoring, and maintaining the Flink Iceberg Consumer application in production environments.

## Table of Contents

- [Kubernetes Deployment](#kubernetes-deployment)
- [Monitoring and Alerting](#monitoring-and-alerting)
- [Querying Aggregate Tables](#querying-aggregate-tables)
- [Troubleshooting](#troubleshooting)
- [Disaster Recovery](#disaster-recovery)
- [Scaling and Performance](#scaling-and-performance)
- [Maintenance Procedures](#maintenance-procedures)

## Kubernetes Deployment

### Prerequisites

Before deploying to Kubernetes, ensure you have:

1. **Kubernetes cluster** (v1.24+) with sufficient resources
2. **Flink Kubernetes Operator** installed
3. **Persistent storage** configured (for checkpoints and state)
4. **Network access** to Kafka, Schema Registry, and S3/MinIO
5. **RBAC permissions** configured

### Installing Flink Kubernetes Operator

```bash
# Create namespace for Flink Operator
kubectl create namespace flink-operator

# Add Flink Operator Helm repository
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.6.0/
helm repo update

# Install Flink Operator
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --namespace flink-operator \
  --set webhook.create=false

# Verify installation
kubectl get pods -n flink-operator
```

### Preparing the Deployment

#### 1. Create Namespace

```bash
kubectl apply -f k8s/namespace.yaml
```

**namespace.yaml:**
```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: data-platform
  labels:
    name: data-platform
    environment: production
```

#### 2. Create Secrets

Store sensitive credentials in Kubernetes secrets:

```bash
# Create secret for Kafka credentials
kubectl create secret generic kafka-credentials \
  --from-literal=username=flink-consumer \
  --from-literal=password=<KAFKA_PASSWORD> \
  -n data-platform

# Create secret for S3/MinIO credentials
kubectl create secret generic s3-credentials \
  --from-literal=access-key=<S3_ACCESS_KEY> \
  --from-literal=secret-key=<S3_SECRET_KEY> \
  -n data-platform

# Or apply from file
kubectl apply -f k8s/secrets.yaml
```

**secrets.yaml:**
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-credentials
  namespace: data-platform
type: Opaque
stringData:
  username: flink-consumer
  password: <KAFKA_PASSWORD>
---
apiVersion: v1
kind: Secret
metadata:
  name: s3-credentials
  namespace: data-platform
type: Opaque
stringData:
  access-key: <S3_ACCESS_KEY>
  secret-key: <S3_SECRET_KEY>
```

#### 3. Create ConfigMap

```bash
kubectl apply -f k8s/configmap.yaml
```

**configmap.yaml:**
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: flink-config
  namespace: data-platform
data:
  flink-conf.yaml: |
    jobmanager.rpc.address: health-data-consumer-rest
    taskmanager.numberOfTaskSlots: 4
    parallelism.default: 6
    state.backend: rocksdb
    state.checkpoints.dir: s3://flink-checkpoints/health-consumer
    execution.checkpointing.interval: 60s
    execution.checkpointing.mode: EXACTLY_ONCE
    execution.checkpointing.timeout: 10min
    execution.checkpointing.max-concurrent-checkpoints: 1
    restart-strategy: fixed-delay
    restart-strategy.fixed-delay.attempts: 3
    restart-strategy.fixed-delay.delay: 10s
    metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
    metrics.reporter.prom.port: 9249
  log4j2.properties: |
    rootLogger.level = INFO
    rootLogger.appenderRef.console.ref = ConsoleAppender
    appender.console.name = ConsoleAppender
    appender.console.type = CONSOLE
    appender.console.layout.type = JsonLayout
    appender.console.layout.compact = true
```

#### 4. Create ServiceAccount and RBAC

```bash
kubectl apply -f k8s/serviceaccount.yaml
```

**serviceaccount.yaml:**
```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: flink-service-account
  namespace: data-platform
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: flink-role
  namespace: data-platform
rules:
  - apiGroups: [""]
    resources: ["pods", "configmaps", "services"]
    verbs: ["get", "list", "watch", "create", "update", "delete", "patch"]
  - apiGroups: ["apps"]
    resources: ["deployments", "replicasets"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: flink-role-binding
  namespace: data-platform
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: flink-role
subjects:
  - kind: ServiceAccount
    name: flink-service-account
    namespace: data-platform
```

### Deploying the Application

#### 1. Deploy FlinkDeployment

```bash
kubectl apply -f k8s/flinkdeployment.yaml
```

**flinkdeployment.yaml:**
```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: health-data-consumer
  namespace: data-platform
spec:
  image: health-stack/flink-iceberg-consumer:1.0.0
  imagePullPolicy: Always
  flinkVersion: v1_18
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
    state.backend: rocksdb
    state.checkpoints.dir: s3://flink-checkpoints/health-consumer
    execution.checkpointing.interval: 60s
    execution.checkpointing.mode: EXACTLY_ONCE
    metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
    metrics.reporter.prom.port: "9249"
  serviceAccount: flink-service-account
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
    replicas: 1
    podTemplate:
      spec:
        containers:
          - name: flink-main-container
            env:
              - name: KAFKA_BROKERS
                value: "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092"
              - name: KAFKA_TOPIC
                value: "health-data-raw"
              - name: KAFKA_GROUP_ID
                value: "flink-iceberg-consumer"
              - name: SCHEMA_REGISTRY_URL
                value: "http://schema-registry:8081"
              - name: ICEBERG_CATALOG_TYPE
                value: "hadoop"
              - name: ICEBERG_WAREHOUSE
                value: "s3a://data-lake/warehouse"
              - name: S3_ENDPOINT
                value: "http://minio:9000"
              - name: S3_ACCESS_KEY
                valueFrom:
                  secretKeyRef:
                    name: s3-credentials
                    key: access-key
              - name: S3_SECRET_KEY
                valueFrom:
                  secretKeyRef:
                    name: s3-credentials
                    key: secret-key
              - name: FLINK_PARALLELISM
                value: "6"
              - name: LOG_LEVEL
                value: "INFO"
  taskManager:
    resource:
      memory: "4096m"
      cpu: 2
    replicas: 3
    podTemplate:
      spec:
        containers:
          - name: flink-main-container
            env:
              - name: KAFKA_BROKERS
                value: "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092"
              - name: KAFKA_TOPIC
                value: "health-data-raw"
              - name: KAFKA_GROUP_ID
                value: "flink-iceberg-consumer"
              - name: SCHEMA_REGISTRY_URL
                value: "http://schema-registry:8081"
              - name: ICEBERG_CATALOG_TYPE
                value: "hadoop"
              - name: ICEBERG_WAREHOUSE
                value: "s3a://data-lake/warehouse"
              - name: S3_ENDPOINT
                value: "http://minio:9000"
              - name: S3_ACCESS_KEY
                valueFrom:
                  secretKeyRef:
                    name: s3-credentials
                    key: access-key
              - name: S3_SECRET_KEY
                valueFrom:
                  secretKeyRef:
                    name: s3-credentials
                    key: secret-key
              - name: FLINK_PARALLELISM
                value: "6"
              - name: LOG_LEVEL
                value: "INFO"
  job:
    jarURI: local:///opt/flink/opt/flink-python_2.12-1.18.0.jar
    args: ["-py", "/opt/flink-app/flink_consumer/main.py"]
    parallelism: 6
    upgradeMode: savepoint
    state: running
    savepointTriggerNonce: 0
```

#### 2. Verify Deployment

```bash
# Check FlinkDeployment status
kubectl get flinkdeployment -n data-platform

# Expected output:
# NAME                   JOB STATUS   LIFECYCLE STATE
# health-data-consumer   RUNNING      STABLE

# Check pods
kubectl get pods -n data-platform

# Expected output:
# NAME                                          READY   STATUS    RESTARTS   AGE
# health-data-consumer-jobmanager-0             1/1     Running   0          5m
# health-data-consumer-taskmanager-1-1          1/1     Running   0          5m
# health-data-consumer-taskmanager-2-1          1/1     Running   0          5m
# health-data-consumer-taskmanager-3-1          1/1     Running   0          5m

# View logs
kubectl logs -n data-platform health-data-consumer-jobmanager-0 -f

# Check job status
kubectl describe flinkdeployment health-data-consumer -n data-platform
```

#### 3. Access Flink Web UI

```bash
# Port-forward to access Flink Web UI
kubectl port-forward -n data-platform svc/health-data-consumer-rest 8081:8081

# Open browser to http://localhost:8081
```

### Updating the Application

#### Rolling Update with Savepoint

```bash
# 1. Trigger savepoint
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"job":{"savepointTriggerNonce": 1}}}'

# 2. Wait for savepoint to complete
kubectl get flinkdeployment health-data-consumer -n data-platform -o jsonpath='{.status.jobStatus.savepointInfo}'

# 3. Update image version
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"image":"health-stack/flink-iceberg-consumer:1.1.0"}}'

# 4. Monitor rollout
kubectl rollout status deployment/health-data-consumer-taskmanager -n data-platform
```

#### Rollback to Previous Version

```bash
# 1. Stop current job
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"job":{"state":"suspended"}}}'

# 2. Rollback image
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"image":"health-stack/flink-iceberg-consumer:1.0.0"}}'

# 3. Resume from savepoint
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"job":{"state":"running","initialSavepointPath":"s3://flink-checkpoints/health-consumer/savepoint-123456"}}}'
```

### Scaling the Application

#### Horizontal Scaling (TaskManagers)

```bash
# Scale TaskManagers
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"taskManager":{"replicas":5}}}'

# Verify scaling
kubectl get pods -n data-platform -l component=taskmanager
```

#### Vertical Scaling (Resources)

```bash
# Increase TaskManager memory and CPU
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"taskManager":{"resource":{"memory":"8192m","cpu":4}}}}'
```

#### Parallelism Tuning

```bash
# Update job parallelism
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"job":{"parallelism":12}}}'
```

## Monitoring and Alerting

### Prometheus Setup

#### 1. Install Prometheus Operator

```bash
# Add Prometheus Helm repository
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

# Install Prometheus Operator
helm install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace
```

#### 2. Create ServiceMonitor for Flink

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: flink-metrics
  namespace: data-platform
spec:
  selector:
    matchLabels:
      app: health-data-consumer
  endpoints:
    - port: metrics
      interval: 30s
      path: /metrics
```

Apply the ServiceMonitor:

```bash
kubectl apply -f k8s/servicemonitor.yaml
```

### Grafana Dashboards

#### 1. Access Grafana

```bash
# Get Grafana admin password
kubectl get secret -n monitoring prometheus-grafana -o jsonpath="{.data.admin-password}" | base64 --decode

# Port-forward to Grafana
kubectl port-forward -n monitoring svc/prometheus-grafana 3000:80

# Open browser to http://localhost:3000
# Login with admin/<password>
```

#### 2. Import Dashboards

Import the following dashboards from `docs/grafana/`:

1. **Flink Overview Dashboard** (ID: flink-overview.json)
   - Job status and health
   - Throughput (records in/out)
   - Latency percentiles (p50, p95, p99)
   - Checkpoint metrics

2. **Kafka Consumer Dashboard** (ID: kafka-consumer.json)
   - Consumer lag per partition
   - Consumption rate
   - Partition distribution

3. **Iceberg Writes Dashboard** (ID: iceberg-writes.json)
   - Write throughput
   - File sizes
   - Commit duration
   - Table sizes

4. **Aggregation Pipeline Dashboard** (ID: aggregation-pipeline.json)
   - Windows processed
   - Records aggregated
   - Window latency
   - Late data statistics

### Alert Rules

Create PrometheusRule for alerting:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: flink-alerts
  namespace: data-platform
spec:
  groups:
    - name: flink_health
      interval: 30s
      rules:
        - alert: FlinkJobDown
          expr: flink_jobmanager_job_uptime == 0
          for: 5m
          labels:
            severity: critical
          annotations:
            summary: "Flink job is down"
            description: "Flink job {{ $labels.job_name }} has been down for more than 5 minutes"
        
        - alert: HighCheckpointDuration
          expr: flink_jobmanager_job_lastCheckpointDuration > 300000
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Checkpoint taking too long"
            description: "Checkpoint duration is {{ $value }}ms (> 5 minutes)"
        
        - alert: HighKafkaLag
          expr: flink_consumer_lag > 100000
          for: 10m
          labels:
            severity: warning
          annotations:
            summary: "High Kafka consumer lag"
            description: "Consumer lag is {{ $value }} messages (> 100k)"
        
        - alert: HighErrorRate
          expr: rate(invalid_records[5m]) > 10
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "High error rate detected"
            description: "Error rate is {{ $value }} errors/second"
        
        - alert: CheckpointFailure
          expr: increase(flink_jobmanager_job_numberOfFailedCheckpoints[10m]) > 3
          for: 5m
          labels:
            severity: critical
          annotations:
            summary: "Multiple checkpoint failures"
            description: "{{ $value }} checkpoint failures in the last 10 minutes"
        
        - alert: LowThroughput
          expr: rate(flink_taskmanager_job_task_numRecordsIn[5m]) < 1000
          for: 10m
          labels:
            severity: warning
          annotations:
            summary: "Low throughput detected"
            description: "Throughput is {{ $value }} records/second (< 1000)"
        
        - alert: HighMemoryUsage
          expr: flink_taskmanager_Status_JVM_Memory_Heap_Used / flink_taskmanager_Status_JVM_Memory_Heap_Max > 0.9
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "High memory usage"
            description: "JVM heap usage is {{ $value | humanizePercentage }}"
```

Apply the alert rules:

```bash
kubectl apply -f k8s/prometheus-rules.yaml
```

### Alertmanager Configuration

Configure Alertmanager to send notifications:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: alertmanager-config
  namespace: monitoring
data:
  alertmanager.yml: |
    global:
      resolve_timeout: 5m
    
    route:
      group_by: ['alertname', 'cluster', 'service']
      group_wait: 10s
      group_interval: 10s
      repeat_interval: 12h
      receiver: 'slack-notifications'
      routes:
        - match:
            severity: critical
          receiver: 'pagerduty-critical'
    
    receivers:
      - name: 'slack-notifications'
        slack_configs:
          - api_url: '<SLACK_WEBHOOK_URL>'
            channel: '#data-platform-alerts'
            title: 'Flink Alert: {{ .GroupLabels.alertname }}'
            text: '{{ range .Alerts }}{{ .Annotations.description }}{{ end }}'
      
      - name: 'pagerduty-critical'
        pagerduty_configs:
          - service_key: '<PAGERDUTY_SERVICE_KEY>'
```

## Querying Aggregate Tables

### Using Spark with Iceberg

#### 1. Start Spark Shell with Iceberg

```bash
spark-shell \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.health_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.health_catalog.type=hadoop \
  --conf spark.sql.catalog.health_catalog.warehouse=s3a://data-lake/warehouse \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true
```

#### 2. Query Daily Aggregates

```sql
-- Get daily heart rate statistics for a user
SELECT 
  user_id,
  aggregation_date,
  min_value,
  max_value,
  avg_value,
  stddev_value,
  count
FROM health_catalog.health_db.health_data_daily_agg
WHERE user_id = 'user-123'
  AND data_type = 'heartRate'
  AND aggregation_date >= '2025-11-01'
ORDER BY aggregation_date DESC;

-- Get daily step counts for multiple users
SELECT 
  user_id,
  aggregation_date,
  sum_value as total_steps,
  avg_value as avg_steps,
  count as measurements
FROM health_catalog.health_db.health_data_daily_agg
WHERE data_type = 'steps'
  AND aggregation_date BETWEEN '2025-11-01' AND '2025-11-17'
ORDER BY user_id, aggregation_date;

-- Find users with abnormal heart rates
SELECT 
  user_id,
  aggregation_date,
  max_value as peak_heart_rate,
  avg_value as avg_heart_rate
FROM health_catalog.health_db.health_data_daily_agg
WHERE data_type = 'heartRate'
  AND (max_value > 200 OR min_value < 40)
  AND aggregation_date >= CURRENT_DATE - INTERVAL 7 DAYS;
```

#### 3. Query Weekly Aggregates

```sql
-- Get weekly trends for a user
SELECT 
  user_id,
  year,
  week_of_year,
  week_start_date,
  week_end_date,
  data_type,
  avg_value,
  min_value,
  max_value
FROM health_catalog.health_db.health_data_weekly_agg
WHERE user_id = 'user-123'
  AND year = 2025
ORDER BY week_of_year DESC;

-- Compare weekly averages across users
SELECT 
  data_type,
  week_start_date,
  COUNT(DISTINCT user_id) as user_count,
  AVG(avg_value) as overall_avg,
  MIN(min_value) as overall_min,
  MAX(max_value) as overall_max
FROM health_catalog.health_db.health_data_weekly_agg
WHERE year = 2025
  AND week_of_year >= 40
GROUP BY data_type, week_start_date
ORDER BY data_type, week_start_date;
```

#### 4. Query Monthly Aggregates

```sql
-- Get monthly summary for a user
SELECT 
  user_id,
  year,
  month,
  data_type,
  avg_value,
  count,
  record_count
FROM health_catalog.health_db.health_data_monthly_agg
WHERE user_id = 'user-123'
  AND year = 2025
ORDER BY month DESC, data_type;

-- Year-over-year comparison
SELECT 
  user_id,
  data_type,
  year,
  month,
  avg_value,
  LAG(avg_value) OVER (PARTITION BY user_id, data_type, month ORDER BY year) as prev_year_avg
FROM health_catalog.health_db.health_data_monthly_agg
WHERE user_id = 'user-123'
  AND data_type = 'heartRate'
ORDER BY year, month;
```

### Using Trino/Presto

```sql
-- Configure Iceberg catalog in Trino
-- Add to catalog/iceberg.properties:
-- connector.name=iceberg
-- iceberg.catalog.type=hadoop
-- hive.metastore.uri=thrift://metastore:9083

-- Query from Trino
SELECT 
  user_id,
  data_type,
  aggregation_date,
  avg_value,
  count
FROM iceberg.health_db.health_data_daily_agg
WHERE aggregation_date >= DATE '2025-11-01'
LIMIT 100;
```

### Using DuckDB (Local Analysis)

```python
import duckdb

# Connect to DuckDB
conn = duckdb.connect()

# Install and load Iceberg extension
conn.execute("INSTALL iceberg")
conn.execute("LOAD iceberg")

# Query Iceberg table
query = """
SELECT 
  user_id,
  aggregation_date,
  data_type,
  avg_value,
  count
FROM iceberg_scan('s3://data-lake/warehouse/health_db/health_data_daily_agg')
WHERE aggregation_date >= '2025-11-01'
ORDER BY aggregation_date DESC
LIMIT 100
"""

result = conn.execute(query).fetchdf()
print(result)
```

### Common Query Patterns

#### Time Series Analysis

```sql
-- 7-day moving average
SELECT 
  user_id,
  data_type,
  aggregation_date,
  avg_value,
  AVG(avg_value) OVER (
    PARTITION BY user_id, data_type 
    ORDER BY aggregation_date 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) as moving_avg_7d
FROM health_catalog.health_db.health_data_daily_agg
WHERE user_id = 'user-123'
  AND data_type = 'heartRate'
ORDER BY aggregation_date;
```

#### Anomaly Detection

```sql
-- Detect outliers using standard deviation
WITH stats AS (
  SELECT 
    user_id,
    data_type,
    AVG(avg_value) as mean_value,
    STDDEV(avg_value) as std_value
  FROM health_catalog.health_db.health_data_daily_agg
  WHERE aggregation_date >= CURRENT_DATE - INTERVAL 30 DAYS
  GROUP BY user_id, data_type
)
SELECT 
  d.user_id,
  d.data_type,
  d.aggregation_date,
  d.avg_value,
  s.mean_value,
  s.std_value,
  ABS(d.avg_value - s.mean_value) / s.std_value as z_score
FROM health_catalog.health_db.health_data_daily_agg d
JOIN stats s ON d.user_id = s.user_id AND d.data_type = s.data_type
WHERE ABS(d.avg_value - s.mean_value) / s.std_value > 3
ORDER BY z_score DESC;
```

#### Cohort Analysis

```sql
-- Compare user cohorts
SELECT 
  CASE 
    WHEN avg_value < 60 THEN 'Low'
    WHEN avg_value BETWEEN 60 AND 100 THEN 'Normal'
    ELSE 'High'
  END as heart_rate_category,
  COUNT(DISTINCT user_id) as user_count,
  AVG(avg_value) as avg_heart_rate
FROM health_catalog.health_db.health_data_daily_agg
WHERE data_type = 'heartRate'
  AND aggregation_date >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY 1
ORDER BY 1;
```

## Troubleshooting

### Common Issues and Solutions

#### Issue: Job Fails to Start

**Symptoms:**
- FlinkDeployment shows `FAILED` state
- JobManager logs show initialization errors

**Diagnosis:**
```bash
# Check FlinkDeployment status
kubectl describe flinkdeployment health-data-consumer -n data-platform

# Check JobManager logs
kubectl logs -n data-platform health-data-consumer-jobmanager-0 --tail=100
```

**Common Causes and Solutions:**

1. **Kafka Connection Failure**
   ```
   Error: Failed to connect to Kafka brokers
   
   Solution:
   - Verify KAFKA_BROKERS environment variable
   - Check network connectivity: kubectl exec -it <pod> -- nc -zv kafka-broker-1 9092
   - Verify Kafka credentials in secrets
   ```

2. **Schema Registry Unavailable**
   ```
   Error: Schema Registry connection timeout
   
   Solution:
   - Verify SCHEMA_REGISTRY_URL
   - Check Schema Registry health: curl http://schema-registry:8081/subjects
   - Ensure network policies allow access
   ```

3. **S3/MinIO Access Denied**
   ```
   Error: Access Denied (Service: Amazon S3; Status Code: 403)
   
   Solution:
   - Verify S3_ACCESS_KEY and S3_SECRET_KEY in secrets
   - Check bucket permissions
   - Verify S3_ENDPOINT configuration
   ```

#### Issue: High Checkpoint Duration

**Symptoms:**
- Checkpoint duration > 5 minutes
- Alert: HighCheckpointDuration firing

**Diagnosis:**
```bash
# Check checkpoint metrics
kubectl exec -n data-platform health-data-consumer-jobmanager-0 -- \
  curl -s http://localhost:8081/jobs/<job-id>/checkpoints

# Check state size
kubectl exec -n data-platform health-data-consumer-jobmanager-0 -- \
  curl -s http://localhost:8081/jobs/<job-id>/checkpoints/details/<checkpoint-id>
```

**Solutions:**

1. **Increase Checkpoint Timeout**
   ```yaml
   flinkConfiguration:
     execution.checkpointing.timeout: 15min
   ```

2. **Enable Incremental Checkpoints**
   ```yaml
   flinkConfiguration:
     state.backend.incremental: "true"
   ```

3. **Tune RocksDB**
   ```yaml
   flinkConfiguration:
     state.backend.rocksdb.block.cache-size: 256m
     state.backend.rocksdb.writebuffer.size: 64m
   ```

4. **Reduce State Size**
   - Enable state TTL for windowed operations
   - Optimize data structures
   - Consider state compaction

#### Issue: High Kafka Consumer Lag

**Symptoms:**
- Consumer lag > 100k messages
- Alert: HighKafkaLag firing
- Slow data processing

**Diagnosis:**
```bash
# Check consumer lag
kafka-consumer-groups.sh --bootstrap-server kafka-broker-1:9092 \
  --group flink-iceberg-consumer --describe

# Check Flink throughput
kubectl exec -n data-platform health-data-consumer-jobmanager-0 -- \
  curl -s http://localhost:8081/jobs/<job-id>/metrics?get=numRecordsInPerSecond
```

**Solutions:**

1. **Increase Parallelism**
   ```bash
   kubectl patch flinkdeployment health-data-consumer -n data-platform \
     --type merge \
     -p '{"spec":{"job":{"parallelism":12}}}'
   ```

2. **Scale TaskManagers**
   ```bash
   kubectl patch flinkdeployment health-data-consumer -n data-platform \
     --type merge \
     -p '{"spec":{"taskManager":{"replicas":5}}}'
   ```

3. **Optimize Iceberg Writes**
   - Increase batch size
   - Reduce commit frequency
   - Tune file size targets

4. **Check for Bottlenecks**
   - Review transformation logic
   - Check validation performance
   - Monitor network I/O

#### Issue: Checkpoint Failures

**Symptoms:**
- Repeated checkpoint failures
- Alert: CheckpointFailure firing
- Job restarts frequently

**Diagnosis:**
```bash
# Check checkpoint history
kubectl exec -n data-platform health-data-consumer-jobmanager-0 -- \
  curl -s http://localhost:8081/jobs/<job-id>/checkpoints/history

# Check TaskManager logs
kubectl logs -n data-platform -l component=taskmanager --tail=100
```

**Solutions:**

1. **S3 Connectivity Issues**
   ```bash
   # Test S3 connectivity from pod
   kubectl exec -it health-data-consumer-taskmanager-1-1 -n data-platform -- \
     aws s3 ls s3://flink-checkpoints/ --endpoint-url http://minio:9000
   ```

2. **Insufficient Resources**
   ```yaml
   taskManager:
     resource:
       memory: "8192m"  # Increase memory
       cpu: 4           # Increase CPU
   ```

3. **Network Timeouts**
   ```yaml
   flinkConfiguration:
     akka.ask.timeout: 60s
     web.timeout: 600000
   ```

#### Issue: Out of Memory Errors

**Symptoms:**
- TaskManager pods restarting
- OOMKilled status
- Memory-related errors in logs

**Diagnosis:**
```bash
# Check pod resource usage
kubectl top pods -n data-platform

# Check memory metrics
kubectl exec -n data-platform health-data-consumer-jobmanager-0 -- \
  curl -s http://localhost:8081/jobs/<job-id>/metrics?get=Status.JVM.Memory.Heap.Used
```

**Solutions:**

1. **Increase TaskManager Memory**
   ```yaml
   taskManager:
     resource:
       memory: "8192m"
   ```

2. **Tune Memory Configuration**
   ```yaml
   flinkConfiguration:
     taskmanager.memory.process.size: 8192m
     taskmanager.memory.flink.size: 6144m
     taskmanager.memory.managed.fraction: 0.4
   ```

3. **Enable Off-Heap Memory**
   ```yaml
   flinkConfiguration:
     taskmanager.memory.managed.size: 2048m
   ```

4. **Optimize State Backend**
   - Use RocksDB for large state
   - Enable incremental checkpoints
   - Configure block cache size

#### Issue: Data Quality Problems

**Symptoms:**
- High invalid_records metric
- Missing data in Iceberg tables
- Incorrect aggregation results

**Diagnosis:**
```bash
# Check validation metrics
kubectl exec -n data-platform health-data-consumer-jobmanager-0 -- \
  curl -s http://localhost:9249/metrics | grep invalid_records

# Query error table
spark-sql --database health_db
SELECT * FROM health_data_errors ORDER BY processing_time DESC LIMIT 100;
```

**Solutions:**

1. **Review Validation Rules**
   - Check validator logic
   - Adjust value ranges
   - Update data type mappings

2. **Check Schema Evolution**
   - Verify schema compatibility
   - Update transformation logic
   - Test with sample data

3. **Monitor DLQ**
   - Analyze error patterns
   - Fix upstream data issues
   - Update validation rules

### Debugging Tools

#### 1. Flink CLI

```bash
# List running jobs
kubectl exec -n data-platform health-data-consumer-jobmanager-0 -- \
  flink list

# Cancel job
kubectl exec -n data-platform health-data-consumer-jobmanager-0 -- \
  flink cancel <job-id>

# Trigger savepoint
kubectl exec -n data-platform health-data-consumer-jobmanager-0 -- \
  flink savepoint <job-id> s3://flink-checkpoints/health-consumer/savepoints
```

#### 2. Metrics API

```bash
# Get all metrics
curl http://localhost:8081/jobs/<job-id>/metrics

# Get specific metric
curl http://localhost:8081/jobs/<job-id>/metrics?get=numRecordsIn

# Get vertex metrics
curl http://localhost:8081/jobs/<job-id>/vertices/<vertex-id>/metrics
```

#### 3. Thread Dump

```bash
# Get thread dump from TaskManager
kubectl exec -n data-platform health-data-consumer-taskmanager-1-1 -- \
  jstack 1 > thread-dump.txt
```

#### 4. Heap Dump

```bash
# Generate heap dump
kubectl exec -n data-platform health-data-consumer-taskmanager-1-1 -- \
  jmap -dump:format=b,file=/tmp/heap-dump.hprof 1

# Copy heap dump locally
kubectl cp data-platform/health-data-consumer-taskmanager-1-1:/tmp/heap-dump.hprof ./heap-dump.hprof
```

## Disaster Recovery

### Backup Strategy

#### 1. Checkpoints

Checkpoints are automatically created by Flink and stored in S3/MinIO:

```yaml
flinkConfiguration:
  state.checkpoints.dir: s3://flink-checkpoints/health-consumer
  execution.checkpointing.interval: 60s
  execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
```

**Retention Policy:**
- Checkpoints are retained for 7 days
- Externalized checkpoints are kept on job cancellation
- Automatic cleanup of old checkpoints

**Verification:**
```bash
# List checkpoints
aws s3 ls s3://flink-checkpoints/health-consumer/ --recursive --endpoint-url http://minio:9000

# Check checkpoint metadata
aws s3 cp s3://flink-checkpoints/health-consumer/<checkpoint-id>/_metadata --endpoint-url http://minio:9000 -
```

#### 2. Savepoints

Manual savepoints for planned maintenance and upgrades:

```bash
# Trigger savepoint
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"job":{"savepointTriggerNonce": 1}}}'

# Wait for savepoint completion
kubectl get flinkdeployment health-data-consumer -n data-platform \
  -o jsonpath='{.status.jobStatus.savepointInfo.location}'

# List savepoints
aws s3 ls s3://flink-checkpoints/health-consumer/savepoints/ --endpoint-url http://minio:9000
```

**Best Practices:**
- Create savepoint before upgrades
- Store savepoints separately from checkpoints
- Retain savepoints indefinitely
- Document savepoint locations

#### 3. Iceberg Snapshots

Iceberg automatically creates snapshots on each commit:

```sql
-- List table snapshots
SELECT * FROM health_catalog.health_db.health_data_raw.snapshots
ORDER BY committed_at DESC;

-- View snapshot details
SELECT * FROM health_catalog.health_db.health_data_raw.history;
```

**Retention Policy:**
```sql
-- Configure snapshot retention (30 days)
ALTER TABLE health_catalog.health_db.health_data_raw
SET TBLPROPERTIES (
  'history.expire.max-snapshot-age-ms' = '2592000000'
);
```

### Recovery Procedures

#### Scenario 1: Application Failure (Automatic Recovery)

Flink automatically recovers from the last successful checkpoint:

```bash
# Check job status
kubectl get flinkdeployment health-data-consumer -n data-platform

# If job is restarting, monitor recovery
kubectl logs -n data-platform health-data-consumer-jobmanager-0 -f | grep "Restoring"

# Verify recovery completed
kubectl exec -n data-platform health-data-consumer-jobmanager-0 -- \
  curl -s http://localhost:8081/jobs/<job-id> | jq '.state'
```

**Expected Behavior:**
- Job restarts automatically
- Recovers from last checkpoint
- Kafka offsets reset to checkpoint position
- Processing resumes without data loss

#### Scenario 2: Corrupted Checkpoint

If checkpoints are corrupted, recover from savepoint:

```bash
# 1. Stop the job
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"job":{"state":"suspended"}}}'

# 2. Identify valid savepoint
aws s3 ls s3://flink-checkpoints/health-consumer/savepoints/ --endpoint-url http://minio:9000

# 3. Resume from savepoint
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"job":{"state":"running","initialSavepointPath":"s3://flink-checkpoints/health-consumer/savepoints/savepoint-123456"}}}'

# 4. Monitor recovery
kubectl logs -n data-platform health-data-consumer-jobmanager-0 -f
```

#### Scenario 3: Data Corruption in Iceberg

Rollback Iceberg table to previous snapshot:

```sql
-- 1. Identify target snapshot
SELECT snapshot_id, committed_at, summary
FROM health_catalog.health_db.health_data_raw.snapshots
ORDER BY committed_at DESC
LIMIT 10;

-- 2. Rollback to snapshot
CALL health_catalog.system.rollback_to_snapshot(
  'health_db.health_data_raw',
  1234567890123456789
);

-- 3. Verify rollback
SELECT COUNT(*) FROM health_catalog.health_db.health_data_raw;

-- 4. For aggregates, rollback each table
CALL health_catalog.system.rollback_to_snapshot('health_db.health_data_daily_agg', <snapshot_id>);
CALL health_catalog.system.rollback_to_snapshot('health_db.health_data_weekly_agg', <snapshot_id>);
CALL health_catalog.system.rollback_to_snapshot('health_db.health_data_monthly_agg', <snapshot_id>);
```

#### Scenario 4: Complete Cluster Failure

Restore from backups in new cluster:

```bash
# 1. Deploy new Flink cluster
kubectl apply -f k8s/flinkdeployment.yaml

# 2. Restore from latest savepoint
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"job":{"initialSavepointPath":"s3://flink-checkpoints/health-consumer/savepoints/latest"}}}'

# 3. Verify Iceberg tables are accessible
spark-sql --database health_db
SHOW TABLES;
SELECT COUNT(*) FROM health_data_raw;

# 4. Monitor job startup
kubectl logs -n data-platform health-data-consumer-jobmanager-0 -f

# 5. Verify data processing
kubectl exec -n data-platform health-data-consumer-jobmanager-0 -- \
  curl -s http://localhost:8081/jobs/<job-id>/metrics?get=numRecordsIn
```

#### Scenario 5: Kafka Offset Reset

If Kafka offsets are lost or incorrect:

```bash
# 1. Stop the Flink job
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"job":{"state":"suspended"}}}'

# 2. Reset Kafka consumer group offsets
kafka-consumer-groups.sh --bootstrap-server kafka-broker-1:9092 \
  --group flink-iceberg-consumer \
  --reset-offsets \
  --to-datetime 2025-11-17T00:00:00.000 \
  --topic health-data-raw \
  --execute

# 3. Resume job (will start from new offsets)
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"job":{"state":"running"}}}'
```

### Backup Verification

Regular backup verification procedures:

```bash
#!/bin/bash
# backup-verification.sh

# 1. Verify checkpoint accessibility
echo "Checking checkpoints..."
aws s3 ls s3://flink-checkpoints/health-consumer/ --endpoint-url http://minio:9000

# 2. Verify savepoint integrity
echo "Checking savepoints..."
LATEST_SAVEPOINT=$(aws s3 ls s3://flink-checkpoints/health-consumer/savepoints/ \
  --endpoint-url http://minio:9000 | sort | tail -n 1 | awk '{print $4}')
echo "Latest savepoint: $LATEST_SAVEPOINT"

# 3. Verify Iceberg snapshots
echo "Checking Iceberg snapshots..."
spark-sql -e "SELECT COUNT(*) as snapshot_count FROM health_catalog.health_db.health_data_raw.snapshots"

# 4. Test recovery (in test environment)
echo "Testing recovery from savepoint..."
# Deploy test job with savepoint
kubectl apply -f k8s/test-recovery.yaml

# 5. Verify data integrity
echo "Verifying data integrity..."
spark-sql -e "SELECT COUNT(*) FROM health_catalog.health_db.health_data_raw"
```

### Disaster Recovery Runbook

**RTO (Recovery Time Objective):** 15 minutes  
**RPO (Recovery Point Objective):** 1 minute (checkpoint interval)

**Step-by-Step Recovery:**

1. **Assess the situation** (2 minutes)
   - Identify failure type
   - Check monitoring dashboards
   - Review recent alerts

2. **Stop affected services** (1 minute)
   ```bash
   kubectl patch flinkdeployment health-data-consumer -n data-platform \
     --type merge -p '{"spec":{"job":{"state":"suspended"}}}'
   ```

3. **Identify recovery point** (2 minutes)
   - Check latest valid checkpoint/savepoint
   - Verify Iceberg snapshot integrity
   - Determine Kafka offset position

4. **Execute recovery** (5 minutes)
   - Deploy new job or restart existing
   - Restore from checkpoint/savepoint
   - Verify Iceberg connectivity

5. **Verify recovery** (3 minutes)
   - Check job status
   - Monitor metrics
   - Verify data processing

6. **Post-recovery validation** (2 minutes)
   - Query Iceberg tables
   - Check data completeness
   - Validate aggregations

7. **Document incident**
   - Record failure cause
   - Document recovery steps
   - Update runbook if needed

## Scaling and Performance

### Horizontal Scaling

#### Scaling TaskManagers

```bash
# Scale up to 5 TaskManagers
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"taskManager":{"replicas":5}}}'

# Scale down to 2 TaskManagers
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"taskManager":{"replicas":2}}}'

# Verify scaling
kubectl get pods -n data-platform -l component=taskmanager
```

**Scaling Guidelines:**
- **Minimum:** 2 TaskManagers (for fault tolerance)
- **Recommended:** 3-5 TaskManagers (for production)
- **Maximum:** Limited by Kafka partitions × 2

#### Adjusting Parallelism

```bash
# Increase parallelism to 12
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"job":{"parallelism":12}}}'
```

**Parallelism Guidelines:**
- **Source parallelism:** Match Kafka partition count (6)
- **Transform parallelism:** 2× source parallelism (12)
- **Sink parallelism:** 4-6 (balance write throughput)
- **Aggregation parallelism:** 
  - Daily: 12 tasks
  - Weekly: 6 tasks
  - Monthly: 3 tasks

### Vertical Scaling

#### Increasing TaskManager Resources

```bash
# Increase memory and CPU
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"taskManager":{"resource":{"memory":"8192m","cpu":4}}}}'
```

**Resource Guidelines:**

| Component | CPU | Memory | Disk |
|-----------|-----|--------|------|
| JobManager | 1-2 cores | 2-4 GB | 10 GB |
| TaskManager (small) | 2 cores | 4 GB | 20 GB |
| TaskManager (medium) | 4 cores | 8 GB | 50 GB |
| TaskManager (large) | 8 cores | 16 GB | 100 GB |

### Performance Tuning

#### 1. Checkpoint Optimization

```yaml
flinkConfiguration:
  # Enable incremental checkpoints
  state.backend.incremental: "true"
  
  # Adjust checkpoint interval
  execution.checkpointing.interval: 60s
  
  # Increase checkpoint timeout
  execution.checkpointing.timeout: 10min
  
  # Tune concurrent checkpoints
  execution.checkpointing.max-concurrent-checkpoints: 1
  
  # Minimum pause between checkpoints
  execution.checkpointing.min-pause: 30s
```

#### 2. RocksDB Tuning

```yaml
flinkConfiguration:
  # Increase block cache
  state.backend.rocksdb.block.cache-size: 512m
  
  # Tune write buffer
  state.backend.rocksdb.writebuffer.size: 128m
  state.backend.rocksdb.writebuffer.count: 4
  
  # Enable bloom filters
  state.backend.rocksdb.predefined-options: SPINNING_DISK_OPTIMIZED_HIGH_MEM
  
  # Compression
  state.backend.rocksdb.compression.per.level: NO_COMPRESSION:NO_COMPRESSION:LZ4_COMPRESSION:LZ4_COMPRESSION:LZ4_COMPRESSION:ZSTD_COMPRESSION:ZSTD_COMPRESSION
```

#### 3. Network Buffer Tuning

```yaml
flinkConfiguration:
  # Increase network buffers
  taskmanager.network.memory.fraction: 0.2
  taskmanager.network.memory.min: 256m
  taskmanager.network.memory.max: 1024m
  
  # Buffer timeout
  execution.buffer-timeout: 100ms
```

#### 4. Kafka Consumer Tuning

```python
# In kafka_source.py
kafka_source = KafkaSource.builder() \
    .set_property("fetch.min.bytes", "1048576")  # 1MB
    .set_property("fetch.max.wait.ms", "500")
    .set_property("max.partition.fetch.bytes", "10485760")  # 10MB
    .set_property("receive.buffer.bytes", "65536")  # 64KB
    .build()
```

#### 5. Iceberg Write Optimization

```python
# In iceberg sink configuration
table_env.execute_sql("""
    CREATE TABLE health_data_raw (
        ...
    ) WITH (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'write.target-file-size-bytes' = '134217728',  -- 128MB
        'write.metadata.compression-codec' = 'gzip',
        'commit.retry.num-retries' = '3',
        'commit.retry.min-wait-ms' = '100'
    )
""")
```

### Performance Monitoring

#### Key Metrics to Monitor

```bash
# Throughput
curl http://localhost:8081/jobs/<job-id>/metrics?get=numRecordsInPerSecond
curl http://localhost:8081/jobs/<job-id>/metrics?get=numRecordsOutPerSecond

# Latency
curl http://localhost:8081/jobs/<job-id>/metrics?get=latency

# Backpressure
curl http://localhost:8081/jobs/<job-id>/vertices/<vertex-id>/backpressure

# Checkpoint duration
curl http://localhost:8081/jobs/<job-id>/metrics?get=lastCheckpointDuration

# Memory usage
curl http://localhost:8081/jobs/<job-id>/metrics?get=Status.JVM.Memory.Heap.Used
```

#### Performance Benchmarks

**Target Performance:**
- **Throughput:** 5,000-10,000 records/second
- **Latency (p99):** < 5 seconds
- **Checkpoint duration:** < 2 minutes
- **Kafka lag:** < 10,000 messages
- **CPU usage:** 60-70% average
- **Memory usage:** < 80% of allocated

### Auto-Scaling (HPA)

Configure Horizontal Pod Autoscaler for TaskManagers:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: flink-taskmanager-hpa
  namespace: data-platform
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: health-data-consumer-taskmanager
  minReplicas: 3
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 80
    - type: Pods
      pods:
        metric:
          name: kafka_consumer_lag
        target:
          type: AverageValue
          averageValue: "50000"
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
        - type: Percent
          value: 50
          periodSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
        - type: Percent
          value: 25
          periodSeconds: 60
```

Apply HPA:

```bash
kubectl apply -f k8s/hpa.yaml

# Monitor HPA
kubectl get hpa -n data-platform -w
```

## Maintenance Procedures

### Routine Maintenance

#### Daily Tasks

```bash
# Check job health
kubectl get flinkdeployment -n data-platform

# Monitor key metrics
kubectl exec -n data-platform health-data-consumer-jobmanager-0 -- \
  curl -s http://localhost:8081/jobs/<job-id>/metrics | grep -E "numRecordsIn|numRecordsOut|lastCheckpointDuration"

# Check for errors
kubectl logs -n data-platform -l app=health-data-consumer --since=24h | grep ERROR
```

#### Weekly Tasks

```bash
# Review checkpoint sizes
aws s3 ls s3://flink-checkpoints/health-consumer/ --recursive --human-readable --endpoint-url http://minio:9000

# Check Iceberg table sizes
spark-sql -e "
  SELECT 
    table_name,
    SUM(file_size_in_bytes) / 1024 / 1024 / 1024 as size_gb,
    COUNT(*) as file_count
  FROM health_catalog.health_db.health_data_raw.files
  GROUP BY table_name
"

# Review aggregation metrics
kubectl exec -n data-platform health-data-consumer-jobmanager-0 -- \
  curl -s http://localhost:9249/metrics | grep windows_processed
```

#### Monthly Tasks

```bash
# Compact Iceberg tables
spark-sql -e "
  CALL health_catalog.system.rewrite_data_files(
    table => 'health_db.health_data_raw',
    strategy => 'binpack',
    options => map('target-file-size-bytes', '536870912')
  )
"

# Expire old snapshots
spark-sql -e "
  CALL health_catalog.system.expire_snapshots(
    table => 'health_db.health_data_raw',
    older_than => TIMESTAMP '2025-10-17 00:00:00',
    retain_last => 10
  )
"

# Clean up old checkpoints
aws s3 rm s3://flink-checkpoints/health-consumer/ \
  --recursive \
  --exclude "*" \
  --include "*/$(date -d '7 days ago' +%Y-%m-%d)*" \
  --endpoint-url http://minio:9000

# Review and update alert thresholds
kubectl edit prometheusrule flink-alerts -n data-platform
```

### Planned Maintenance

#### Application Upgrade

```bash
# 1. Create savepoint
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"job":{"savepointTriggerNonce": 1}}}'

# 2. Wait for savepoint
SAVEPOINT=$(kubectl get flinkdeployment health-data-consumer -n data-platform \
  -o jsonpath='{.status.jobStatus.savepointInfo.location}')
echo "Savepoint location: $SAVEPOINT"

# 3. Update image
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"image":"health-stack/flink-iceberg-consumer:1.1.0"}}'

# 4. Monitor upgrade
kubectl rollout status deployment/health-data-consumer-taskmanager -n data-platform

# 5. Verify job health
kubectl logs -n data-platform health-data-consumer-jobmanager-0 -f
```

#### Flink Version Upgrade

```bash
# 1. Test new version in staging
kubectl apply -f k8s/staging/flinkdeployment-v1.19.yaml

# 2. Create production savepoint
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"job":{"savepointTriggerNonce": 1}}}'

# 3. Update FlinkDeployment with new version
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"flinkVersion":"v1_19","image":"health-stack/flink-iceberg-consumer:1.1.0-flink1.19"}}'

# 4. Monitor and validate
kubectl logs -n data-platform health-data-consumer-jobmanager-0 -f
```

#### Kubernetes Cluster Upgrade

```bash
# 1. Create savepoint
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"job":{"savepointTriggerNonce": 1}}}'

# 2. Drain nodes one by one
kubectl drain <node-name> --ignore-daemonsets --delete-emptydir-data

# 3. Upgrade node

# 4. Uncordon node
kubectl uncordon <node-name>

# 5. Verify pods rescheduled
kubectl get pods -n data-platform -o wide

# 6. Repeat for all nodes
```

### Emergency Procedures

#### Emergency Stop

```bash
# Stop job immediately
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"job":{"state":"suspended"}}}'

# Delete deployment (if needed)
kubectl delete flinkdeployment health-data-consumer -n data-platform
```

#### Emergency Rollback

```bash
# Rollback to previous version
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"image":"health-stack/flink-iceberg-consumer:1.0.0"}}'

# Restore from savepoint
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"job":{"initialSavepointPath":"s3://flink-checkpoints/health-consumer/savepoints/savepoint-123456"}}}'
```

## Appendix

### Useful Commands Reference

```bash
# Get job ID
kubectl exec -n data-platform health-data-consumer-jobmanager-0 -- \
  flink list | grep "health-data-consumer" | awk '{print $4}'

# Get all metrics
curl http://localhost:8081/jobs/<job-id>/metrics

# Get vertex IDs
curl http://localhost:8081/jobs/<job-id> | jq '.vertices[].id'

# Check backpressure
curl http://localhost:8081/jobs/<job-id>/vertices/<vertex-id>/backpressure

# Get task manager details
curl http://localhost:8081/taskmanagers

# Get configuration
curl http://localhost:8081/jobmanager/config
```

### Contact Information

- **On-Call Engineer:** [PagerDuty rotation]
- **Slack Channel:** #data-platform-alerts
- **Documentation:** https://docs.health-stack.io/flink-consumer
- **Runbook:** https://runbook.health-stack.io/flink-consumer

### Change Log

| Date | Version | Changes | Author |
|------|---------|---------|--------|
| 2025-11-17 | 1.0.0 | Initial operations guide | Data Platform Team |
