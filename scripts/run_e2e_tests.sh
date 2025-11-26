#!/bin/bash
set -e

echo "Starting end-to-end test environment..."

# Start test infrastructure
docker-compose -f docker-compose.test.yml up -d

echo "Waiting for services to be ready..."
sleep 15

# Wait for gateway health check
echo "Checking gateway health..."
max_retries=30
retry_count=0

while [ $retry_count -lt $max_retries ]; do
    if curl -f http://localhost:3001/health > /dev/null 2>&1; then
        echo "Gateway is healthy!"
        break
    fi
    echo "Waiting for gateway... ($retry_count/$max_retries)"
    sleep 2
    retry_count=$((retry_count + 1))
done

if [ $retry_count -eq $max_retries ]; then
    echo "Gateway failed to become healthy"
    docker-compose -f docker-compose.test.yml logs gateway-test
    docker-compose -f docker-compose.test.yml down
    exit 1
fi

# Run E2E tests only
echo "Running end-to-end tests..."
cd gateway

# Set test environment variables
export TEST_GATEWAY_URL="http://localhost:3001"
export TEST_KAFKA_BROKERS="localhost:19095"
export TEST_SCHEMA_REGISTRY_URL="http://localhost:8082"

# Run pytest with E2E marker
python -m pytest tests/test_e2e_pipeline.py -v --tb=short -m e2e

test_exit_code=$?

# Cleanup
echo "Cleaning up test environment..."
cd ..
docker-compose -f docker-compose.test.yml down -v

if [ $test_exit_code -eq 0 ]; then
    echo "✅ All E2E tests passed!"
else
    echo "❌ E2E tests failed"
fi

exit $test_exit_code
