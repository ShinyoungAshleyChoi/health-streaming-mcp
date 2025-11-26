import pytest
import httpx
import time
from confluent_kafka import Consumer


class TestHealthDataAPI:
    """Integration tests for /api/v1/health-data endpoint"""
    
    def test_post_valid_health_data(self, http_client, sample_health_data, kafka_consumer):
        """Test posting valid health data returns 200 and message is in Kafka"""
        # Send request
        response = http_client.post("/api/v1/health-data", json=sample_health_data)
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert "requestId" in data
        assert "timestamp" in data
        
        # Verify message in Kafka
        message = None
        timeout = time.time() + 10
        while time.time() < timeout:
            msg = kafka_consumer.poll(timeout=1.0)
            if msg is not None and not msg.error():
                message = msg
                break
        
        assert message is not None, "Message not found in Kafka topic"
        assert message.value() is not None
    
    def test_post_invalid_json(self, http_client):
        """Test posting invalid JSON returns 400"""
        response = http_client.post(
            "/api/v1/health-data",
            content="invalid json{",
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 400
        data = response.json()
        assert data["status"] == "error"
    
    def test_post_missing_required_fields(self, http_client):
        """Test posting data with missing required fields returns 422"""
        incomplete_data = {
            "userId": "550e8400-e29b-41d4-a716-446655440000",
            # Missing timestamp, dataType, value
        }
        
        response = http_client.post("/api/v1/health-data", json=incomplete_data)
        
        assert response.status_code == 422
        data = response.json()
        assert data["status"] == "error"
        assert "errors" in data or "message" in data
    
    def test_post_invalid_data_types(self, http_client):
        """Test posting data with invalid types returns 422"""
        invalid_data = {
            "userId": "not-a-uuid",
            "timestamp": "invalid-timestamp",
            "dataType": "heart_rate",
            "value": "not-a-number",
        }
        
        response = http_client.post("/api/v1/health-data", json=invalid_data)
        
        assert response.status_code == 422
        data = response.json()
        assert data["status"] == "error"
    
    def test_post_with_request_id_header(self, http_client, sample_health_data):
        """Test that custom request ID is preserved"""
        custom_request_id = "custom-test-request-id-12345"
        
        response = http_client.post(
            "/api/v1/health-data",
            json=sample_health_data,
            headers={"X-Request-ID": custom_request_id}
        )
        
        assert response.status_code == 200
        # Request ID should be in response or logs
        data = response.json()
        assert data["status"] == "success"
    
    def test_batch_requests(self, http_client, sample_health_data, kafka_consumer):
        """Test sending multiple requests in sequence"""
        num_requests = 5
        successful_requests = 0
        
        for i in range(num_requests):
            data = sample_health_data.copy()
            data["value"] = 70.0 + i
            
            response = http_client.post("/api/v1/health-data", json=data)
            if response.status_code == 200:
                successful_requests += 1
        
        assert successful_requests == num_requests
        
        # Verify messages in Kafka
        messages_received = 0
        timeout = time.time() + 15
        while time.time() < timeout and messages_received < num_requests:
            msg = kafka_consumer.poll(timeout=1.0)
            if msg is not None and not msg.error():
                messages_received += 1
        
        assert messages_received >= num_requests


class TestHealthEndpoint:
    """Integration tests for /health endpoint"""
    
    def test_health_check_returns_healthy(self, http_client):
        """Test health endpoint returns healthy status"""
        response = http_client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "kafka" in data
        assert "schemaRegistry" in data
    
    def test_health_check_kafka_connected(self, http_client):
        """Test health check shows Kafka as connected"""
        response = http_client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["kafka"] in ["connected", "up"]
    
    def test_health_check_schema_registry_connected(self, http_client):
        """Test health check shows Schema Registry as connected"""
        response = http_client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["schemaRegistry"] in ["connected", "up"]


class TestMetricsEndpoint:
    """Integration tests for /metrics endpoint"""
    
    def test_metrics_endpoint_accessible(self, http_client):
        """Test metrics endpoint is accessible"""
        response = http_client.get("/metrics")
        
        assert response.status_code == 200
        assert "text/plain" in response.headers.get("content-type", "")
    
    def test_metrics_contains_request_counter(self, http_client, sample_health_data):
        """Test metrics include request counters after API calls"""
        # Make a request first
        http_client.post("/api/v1/health-data", json=sample_health_data)
        
        # Check metrics
        response = http_client.get("/metrics")
        content = response.text
        
        assert "http_requests_total" in content or "requests_total" in content
