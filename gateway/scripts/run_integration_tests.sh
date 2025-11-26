#!/bin/bash
set -e

echo "🚀 Starting integration test environment..."

# Start test infrastructure
docker-compose -f docker-compose.test.yml up -d

echo "⏳ Waiting for services to be ready..."
sleep 15

# Wait for gateway health check
echo "🔍 Checking gateway health..."
max_retries=30
retry_count=0

while [ $retry_count -lt $max_retries ]; do
    if curl -f http://localhost:3001/health > /dev/null 2>&1; then
        echo "✅ Gateway is healthy"
        break
    fi
    retry_count=$((retry_count + 1))
    echo "Waiting for gateway... ($retry_count/$max_retries)"
    sleep 2
done

if [ $retry_count -eq $max_retries ]; then
    echo "❌ Gateway failed to become healthy"
    docker-compose -f docker-compose.test.yml logs gateway-test
    docker-compose -f docker-compose.test.yml down
    exit 1
fi

# Run tests
echo "🧪 Running integration tests..."
cd gateway

# Install test dependencies
pip install -e ".[test]" > /dev/null 2>&1 || true

# Set test environment variables
export TEST_GATEWAY_URL="http://localhost:3001"
export TEST_KAFKA_BROKERS="localhost:19095"
export TEST_SCHEMA_REGISTRY_URL="http://localhost:8082"

# Run pytest
pytest tests/ -v --tb=short

test_exit_code=$?

# Cleanup
echo "🧹 Cleaning up test environment..."
cd ..
docker-compose -f docker-compose.test.yml down -v

if [ $test_exit_code -eq 0 ]; then
    echo "✅ All tests passed!"
else
    echo "❌ Tests failed"
fi

exit $test_exit_code
