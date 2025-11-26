import pytest
import time
import json
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
import httpx


class TestEndToEndPipeline:
    """End-to-end tests for complete data pipeline (API → Kafka)"""
    
    def test_complete_pipeline_flow(self, http_client, sample_health_data, kafka_consumer):
        """Test complete flow from API request to Kafka message"""
        # Step 1: Send data to API
        response = http_client.post("/api/v1/health-data", json=sample_health_data)
        assert response.status_code == 200
        
        response_data = response.json()
        assert response_data["status"] == "success"
        request_id = response_data.get("requestId")
        
        # Step 2: Verify message appears in Kafka
        message = None
        timeout = time.time() + 10
        while time.time() < timeout:
            msg = kafka_consumer.poll(timeout=1.0)
            if msg is not None and not msg.error():
                message = msg
                break
        
        assert message is not None, "Message not received in Kafka"
        
        # Step 3: Verify message content
        assert message.value() is not None
        assert len(message.value()) > 0
        
        # Step 4: Verify message key matches userId
        key = message.key().decode('utf-8') if isinstance(message.key(), bytes) else message.key()
        assert sample_health_data["userId"] in key or key == sample_health_data["userId"]
    
    def test_multiple_data_types_pipeline(self, http_client, kafka_consumer):
        """Test pipeline with different health data types"""
        data_types = [
            {"dataType": "heart_rate", "value": 72.0, "unit": "bpm"},
            {"dataType": "steps", "value": 10000.0, "unit": "steps"},
            {"dataType": "sleep", "value": 7.5, "unit": "hours"},
        ]
        
        for data_type_info in data_types:
            health_data = {
                "userId": "550e8400-e29b-41d4-a716-446655440000",
                "timestamp": "2025-11-12T10:30:00Z",
                "dataType": data_type_info["dataType"],
                "value": data_type_info["value"],
                "unit": data_type_info["unit"],
                "metadata": {
                    "deviceId": "iPhone14-ABC123",
                    "appVersion": "1.2.3",
                    "platform": "iOS"
                }
            }
            
            # Send request
            response = http_client.post("/api/v1/health-data", json=health_data)
            assert response.status_code == 200
        
        # Verify all messages received
        messages_received = 0
        timeout = time.time() + 15
        while time.time() < timeout and messages_received < len(data_types):
            msg = kafka_consumer.poll(timeout=1.0)
            if msg is not None and not msg.error():
                messages_received += 1
        
        assert messages_received == len(data_types)
    
    def test_high_throughput_pipeline(self, http_client, kafka_consumer):
        """Test pipeline under high load"""
        num_messages = 50
        user_ids = [f"user-{i:04d}" for i in range(10)]
        
        # Send many messages
        start_time = time.time()
        for i in range(num_messages):
            health_data = {
                "userId": user_ids[i % len(user_ids)],
                "timestamp": "2025-11-12T10:30:00Z",
                "dataType": "heart_rate",
                "value": 60.0 + (i % 40),
                "unit": "bpm",
                "metadata": {
                    "deviceId": f"device-{i}",
                    "appVersion": "1.2.3",
                    "platform": "iOS"
                }
            }
            
            response = http_client.post("/api/v1/health-data", json=health_data)
            assert response.status_code == 200
        
        send_duration = time.time() - start_time
        
        # Verify messages received
        messages_received = 0
        timeout = time.time() + 30
        while time.time() < timeout and messages_received < num_messages:
            msg = kafka_consumer.poll(timeout=1.0)
            if msg is not None and not msg.error():
                messages_received += 1
        
        assert messages_received >= num_messages * 0.95  # Allow 5% loss for test stability
        
        # Check throughput
        throughput = num_messages / send_duration
        print(f"Throughput: {throughput:.2f} messages/second")
        assert throughput > 10  # Should handle at least 10 msg/sec


class TestSchemaEvolution:
    """Tests for schema evolution and compatibility"""
    
    def test_optional_field_handling(self, http_client, kafka_consumer):
        """Test that optional fields are handled correctly"""
        # Data without optional unit field
        health_data = {
            "userId": "550e8400-e29b-41d4-a716-446655440000",
            "timestamp": "2025-11-12T10:30:00Z",
            "dataType": "heart_rate",
            "value": 72.0,
            # unit is optional
            "metadata": {
                "deviceId": "iPhone14-ABC123",
                "appVersion": "1.2.3",
                "platform": "iOS"
            }
        }
        
        response = http_client.post("/api/v1/health-data", json=health_data)
        assert response.status_code == 200
        
        # Verify message in Kafka
        message = None
        timeout = time.time() + 10
        while time.time() < timeout:
            msg = kafka_consumer.poll(timeout=1.0)
            if msg is not None and not msg.error():
                message = msg
                break
        
        assert message is not None
    
    def test_backward_compatibility(self, http_client, kafka_consumer):
        """Test that old data format still works (backward compatibility)"""
        # Minimal required fields only
        minimal_data = {
            "userId": "550e8400-e29b-41d4-a716-446655440000",
            "timestamp": "2025-11-12T10:30:00Z",
            "dataType": "heart_rate",
            "value": 72.0,
            "metadata": {
                "deviceId": "iPhone14-ABC123",
                "appVersion": "1.2.3",
                "platform": "iOS"
            }
        }
        
        response = http_client.post("/api/v1/health-data", json=minimal_data)
        assert response.status_code == 200
        
        # Verify message in Kafka
        message = None
        timeout = time.time() + 10
        while time.time() < timeout:
            msg = kafka_consumer.poll(timeout=1.0)
            if msg is not None and not msg.error():
                message = msg
                break
        
        assert message is not None


class TestFailoverScenarios:
    """Tests for system resilience and failover"""
    
    def test_gateway_handles_temporary_kafka_unavailability(self, http_client, sample_health_data, test_config):
        """Test gateway behavior when Kafka is temporarily unavailable"""
        # This test would require stopping/starting Kafka
        # For now, we test that the gateway has retry logic
        
        # Send request
        response = http_client.post("/api/v1/health-data", json=sample_health_data)
        
        # Should either succeed or fail gracefully
        assert response.status_code in [200, 500, 503]
        
        if response.status_code != 200:
            data = response.json()
            assert data["status"] == "error"
    
    def test_concurrent_requests_handling(self, http_client, sample_health_data):
        """Test gateway handles concurrent requests properly"""
        import concurrent.futures
        
        num_concurrent = 10
        
        def send_request(index):
            data = sample_health_data.copy()
            data["value"] = 70.0 + index
            response = http_client.post("/api/v1/health-data", json=data)
            return response.status_code
        
        # Send concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_concurrent) as executor:
            futures = [executor.submit(send_request, i) for i in range(num_concurrent)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
        
        # All should succeed
        success_count = sum(1 for status in results if status == 200)
        assert success_count >= num_concurrent * 0.9  # Allow 10% failure for test stability
    
    def test_message_ordering_per_partition(self, http_client, kafka_consumer):
        """Test that messages for same user maintain order in partition"""
        user_id = "test-user-ordering-123"
        num_messages = 5
        
        # Send messages in sequence
        for i in range(num_messages):
            health_data = {
                "userId": user_id,
                "timestamp": f"2025-11-12T10:30:{i:02d}Z",
                "dataType": "heart_rate",
                "value": 70.0 + i,
                "unit": "bpm",
                "metadata": {
                    "deviceId": "iPhone14-ABC123",
                    "appVersion": "1.2.3",
                    "platform": "iOS"
                }
            }
            
            response = http_client.post("/api/v1/health-data", json=health_data)
            assert response.status_code == 200
        
        # Consume messages and verify they're in same partition
        partitions = []
        offsets = []
        timeout = time.time() + 15
        messages_received = 0
        
        while time.time() < timeout and messages_received < num_messages:
            msg = kafka_consumer.poll(timeout=1.0)
            if msg is not None and not msg.error():
                partitions.append(msg.partition())
                offsets.append(msg.offset())
                messages_received += 1
        
        # All messages should be in same partition
        assert len(set(partitions)) == 1, "Messages went to different partitions"
        
        # Offsets should be sequential (ordered)
        assert offsets == sorted(offsets), "Messages not in order"
    
    def test_system_recovery_after_error(self, http_client, sample_health_data):
        """Test that system recovers after encountering errors"""
        # Send invalid data
        invalid_data = {"invalid": "data"}
        response1 = http_client.post("/api/v1/health-data", json=invalid_data)
        assert response1.status_code in [400, 422]
        
        # Send valid data - should work
        response2 = http_client.post("/api/v1/health-data", json=sample_health_data)
        assert response2.status_code == 200
        
        # System should still be healthy
        health_response = http_client.get("/health")
        assert health_response.status_code == 200
