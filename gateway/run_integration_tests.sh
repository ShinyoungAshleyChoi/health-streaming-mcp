#!/bin/bash

# Integration Test Runner Script
# This script sets up the test environment and runs integration tests

set -e

echo "🚀 Starting Health Stack Gateway Integration Tests"
echo "=================================================="

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker and try again."
    exit 1
fi

print_status "Docker is running"

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    print_error "docker-compose is not installed. Please install it and try again."
    exit 1
fi

print_status "docker-compose is available"

# Clean up any existing test environment
echo ""
echo "🧹 Cleaning up existing test environment..."
docker-compose -f docker-compose.test.yml down -v --remove-orphans 2>/dev/null || true
print_status "Cleanup complete"

# Start test environment
echo ""
echo "🐳 Starting test environment..."
docker-compose -f docker-compose.test.yml up -d

# Wait for services to be healthy
echo ""
echo "⏳ Waiting for services to be ready..."
MAX_WAIT=120
WAIT_TIME=0
SLEEP_INTERVAL=5

while [ $WAIT_TIME -lt $MAX_WAIT ]; do
    if docker-compose -f docker-compose.test.yml ps | grep -q "unhealthy"; then
        echo "   Waiting... (${WAIT_TIME}s/${MAX_WAIT}s)"
        sleep $SLEEP_INTERVAL
        WAIT_TIME=$((WAIT_TIME + SLEEP_INTERVAL))
    else
        # Check if gateway is responding
        if curl -s http://localhost:3001/health > /dev/null 2>&1; then
            print_status "All services are ready"
            break
        else
            echo "   Waiting for gateway... (${WAIT_TIME}s/${MAX_WAIT}s)"
            sleep $SLEEP_INTERVAL
            WAIT_TIME=$((WAIT_TIME + SLEEP_INTERVAL))
        fi
    fi
done

if [ $WAIT_TIME -ge $MAX_WAIT ]; then
    print_error "Services failed to start within ${MAX_WAIT} seconds"
    echo ""
    echo "📋 Service logs:"
    docker-compose -f docker-compose.test.yml logs --tail=50
    docker-compose -f docker-compose.test.yml down -v
    exit 1
fi

# Display service status
echo ""
echo "📊 Service Status:"
docker-compose -f docker-compose.test.yml ps

# Check gateway health
echo ""
echo "🏥 Checking gateway health..."
HEALTH_RESPONSE=$(curl -s http://localhost:3001/health)
echo "$HEALTH_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$HEALTH_RESPONSE"

# Install test dependencies if needed
echo ""
echo "📦 Checking test dependencies..."
cd gateway
if ! python3 -c "import pytest" 2>/dev/null; then
    print_warning "Installing test dependencies..."
    pip install pytest pytest-asyncio httpx confluent-kafka fastavro
fi
print_status "Test dependencies ready"

# Run tests
echo ""
echo "🧪 Running integration tests..."
echo "================================"

# Set test environment variables
export TEST_GATEWAY_URL=http://localhost:3001
export TEST_KAFKA_BROKERS=localhost:19095
export TEST_SCHEMA_REGISTRY_URL=http://localhost:8082

# Run pytest with coverage
if pytest tests/ -v --tb=short; then
    TEST_RESULT=0
    print_status "All tests passed!"
else
    TEST_RESULT=1
    print_error "Some tests failed"
fi

# Cleanup
echo ""
echo "🧹 Cleaning up test environment..."
cd ..
docker-compose -f docker-compose.test.yml down -v

if [ $TEST_RESULT -eq 0 ]; then
    echo ""
    print_status "Integration tests completed successfully! 🎉"
    exit 0
else
    echo ""
    print_error "Integration tests failed"
    exit 1
fi
