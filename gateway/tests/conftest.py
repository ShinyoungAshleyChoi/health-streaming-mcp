import pytest
import os
from typing import Generator
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
import httpx
import time


@pytest.fixture(scope="session")
def test_config():
    """Test configuration"""
    return {
        "gateway_url": os.getenv("TEST_GATEWAY_URL", "http://localhost:3001"),
        "kafka_brokers": os.getenv("TEST_KAFKA_BROKERS", "localhost:19095"),
        "schema_registry_url": os.getenv("TEST_SCHEMA_REGISTRY_URL", "http://localhost:8082"),
        "kafka_topic": "health-data-test",
        "kafka_dlq_topic": "health-data-dlq-test",
    }


@pytest.fixture(scope="session")
def kafka_admin(test_config):
    """Kafka admin client for test setup"""
    admin = AdminClient({"bootstrap.servers": test_config["kafka_brokers"]})
    
    # Create test topics
    topics = [
        NewTopic(test_config["kafka_topic"], num_partitions=3, replication_factor=1),
        NewTopic(test_config["kafka_dlq_topic"], num_partitions=1, replication_factor=1),
    ]
    
    fs = admin.create_topics(topics)
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Topic {topic} creation failed or already exists: {e}")
    
    yield admin
    
    # Cleanup topics after tests
    admin.delete_topics([test_config["kafka_topic"], test_config["kafka_dlq_topic"]])


@pytest.fixture
def kafka_consumer(test_config, kafka_admin) -> Generator[Consumer, None, None]:
    """Kafka consumer for message verification"""
    consumer = Consumer({
        "bootstrap.servers": test_config["kafka_brokers"],
        "group.id": "test-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    
    consumer.subscribe([test_config["kafka_topic"]])
    
    yield consumer
    
    consumer.close()


@pytest.fixture
def dlq_consumer(test_config, kafka_admin) -> Generator[Consumer, None, None]:
    """Kafka consumer for DLQ verification"""
    consumer = Consumer({
        "bootstrap.servers": test_config["kafka_brokers"],
        "group.id": "test-dlq-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    
    consumer.subscribe([test_config["kafka_dlq_topic"]])
    
    yield consumer
    
    consumer.close()


@pytest.fixture
def http_client(test_config) -> Generator[httpx.Client, None, None]:
    """HTTP client for API testing"""
    with httpx.Client(base_url=test_config["gateway_url"], timeout=30.0) as client:
        # Wait for gateway to be ready
        max_retries = 30
        for i in range(max_retries):
            try:
                response = client.get("/health")
                if response.status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(1)
        
        yield client


@pytest.fixture
def sample_health_data():
    """Sample valid health data"""
    return {
        "userId": "550e8400-e29b-41d4-a716-446655440000",
        "timestamp": "2025-11-12T10:30:00Z",
        "dataType": "heart_rate",
        "value": 72.0,
        "unit": "bpm",
        "metadata": {
            "deviceId": "iPhone14-ABC123",
            "appVersion": "1.2.3",
            "platform": "iOS"
        }
    }


@pytest.fixture
def invalid_health_data():
    """Sample invalid health data for testing validation"""
    return {
        "userId": "invalid-uuid",
        "timestamp": "invalid-timestamp",
        "dataType": "unknown_type",
        "value": "not-a-number",
    }
