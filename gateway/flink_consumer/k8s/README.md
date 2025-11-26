# Kubernetes Deployment for Flink Iceberg Consumer

This directory contains Kubernetes manifests for deploying the Flink Iceberg Consumer application using the Flink Kubernetes Operator.

## Prerequisites

1. Kubernetes cluster (v1.24+)
2. Flink Kubernetes Operator installed
3. kubectl configured to access your cluster
4. Docker image built and pushed to registry

## Installation Steps

### 1. Install Flink Kubernetes Operator

```bash
# Add Flink Operator Helm repository
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.7.0/
helm repo update

# Install the operator
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --namespace flink-operator \
  --create-namespace
```

### 2. Build and Push Docker Image

```bash
# Build the image
cd flink_consumer
docker build -t your-registry/flink-iceberg-consumer:1.0.0 -f Dockerfile --target production .

# Push to registry
docker push your-registry/flink-iceberg-consumer:1.0.0
```

### 3. Update Configuration

Edit the manifests to match your environment:

- `secrets.yaml`: Update Kafka and S3 credentials
- `flinkdeployment.yaml`: Update image name and environment variables
- `configmap.yaml`: Adjust Flink configuration as needed

### 4. Deploy to Kubernetes

```bash
# Create namespace
kubectl apply -f namespace.yaml

# Create service account and RBAC
kubectl apply -f serviceaccount.yaml

# Create secrets (update with real credentials first!)
kubectl apply -f secrets.yaml

# Create ConfigMap
kubectl apply -f configmap.yaml

# Deploy Flink application
kubectl apply -f flinkdeployment.yaml
```

## Verify Deployment

```bash
# Check FlinkDeployment status
kubectl get flinkdeployment -n data-platform

# Check pods
kubectl get pods -n data-platform

# View JobManager logs
kubectl logs -n data-platform -l app=health-data-consumer,component=jobmanager

# View TaskManager logs
kubectl logs -n data-platform -l app=health-data-consumer,component=taskmanager

# Access Flink Web UI (port-forward)
kubectl port-forward -n data-platform svc/health-data-consumer-rest 8081:8081
# Open http://localhost:8081
```

## Scaling

```bash
# Scale TaskManagers
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"taskManager":{"replicas":5}}}'
```

## Savepoint and Upgrade

```bash
# Trigger savepoint
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"job":{"savepointTriggerNonce":'$(($(date +%s)))'}}}'

# Upgrade with savepoint
kubectl patch flinkdeployment health-data-consumer -n data-platform \
  --type merge \
  -p '{"spec":{"image":"your-registry/flink-iceberg-consumer:1.1.0","job":{"upgradeMode":"savepoint"}}}'
```

## Monitoring

```bash
# Access Prometheus metrics
kubectl port-forward -n data-platform svc/health-data-consumer-rest 9249:9249
# Metrics available at http://localhost:9249
```

## Troubleshooting

### Check FlinkDeployment status
```bash
kubectl describe flinkdeployment health-data-consumer -n data-platform
```

### View events
```bash
kubectl get events -n data-platform --sort-by='.lastTimestamp'
```

### Check logs
```bash
# JobManager logs
kubectl logs -n data-platform -l component=jobmanager --tail=100

# TaskManager logs
kubectl logs -n data-platform -l component=taskmanager --tail=100
```

### Common Issues

1. **Image pull errors**: Ensure image is pushed to registry and imagePullSecrets are configured
2. **Checkpoint failures**: Verify S3 credentials and bucket permissions
3. **Kafka connection issues**: Check network policies and service DNS resolution

## Cleanup

```bash
# Delete FlinkDeployment
kubectl delete flinkdeployment health-data-consumer -n data-platform

# Delete all resources
kubectl delete -f .

# Delete namespace
kubectl delete namespace data-platform
```

## Production Considerations

1. **High Availability**: Enable HA mode in flink-conf.yaml
2. **Resource Limits**: Set appropriate CPU/memory limits
3. **Monitoring**: Integrate with Prometheus/Grafana
4. **Alerting**: Configure alerts for checkpoint failures, high lag, etc.
5. **Backup**: Regular savepoints and checkpoint retention
6. **Security**: Use proper RBAC, network policies, and secret management
