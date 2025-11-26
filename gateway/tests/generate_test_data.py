#!/usr/bin/env python3
"""
Test data generation script for health-stack gateway integration tests
"""

import json
import random
from datetime import datetime, timedelta
from typing import List, Dict
import uuid


def generate_user_id() -> str:
    """Generate a random UUID for userId"""
    return str(uuid.uuid4())


def generate_timestamp(base_time: datetime = None, offset_seconds: int = 0) -> str:
    """Generate ISO8601 timestamp"""
    if base_time is None:
        base_time = datetime.utcnow()
    
    timestamp = base_time + timedelta(seconds=offset_seconds)
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_heart_rate_data(user_id: str, timestamp: str) -> Dict:
    """Generate heart rate health data"""
    return {
        "userId": user_id,
        "timestamp": timestamp,
        "dataType": "heart_rate",
        "value": random.uniform(60.0, 100.0),
        "unit": "bpm",
        "metadata": {
            "deviceId": f"iPhone14-{random.randint(1000, 9999)}",
            "appVersion": "1.2.3",
            "platform": "iOS"
        }
    }


def generate_steps_data(user_id: str, timestamp: str) -> Dict:
    """Generate steps health data"""
    return {
        "userId": user_id,
        "timestamp": timestamp,
        "dataType": "steps",
        "value": float(random.randint(0, 20000)),
        "unit": "steps",
        "metadata": {
            "deviceId": f"iPhone14-{random.randint(1000, 9999)}",
            "appVersion": "1.2.3",
            "platform": "iOS"
        }
    }


def generate_sleep_data(user_id: str, timestamp: str) -> Dict:
    """Generate sleep health data"""
    return {
        "userId": user_id,
        "timestamp": timestamp,
        "dataType": "sleep",
        "value": random.uniform(4.0, 10.0),
        "unit": "hours",
        "metadata": {
            "deviceId": f"iPhone14-{random.randint(1000, 9999)}",
            "appVersion": "1.2.3",
            "platform": "iOS"
        }
    }


def generate_blood_pressure_data(user_id: str, timestamp: str) -> Dict:
    """Generate blood pressure health data"""
    return {
        "userId": user_id,
        "timestamp": timestamp,
        "dataType": "blood_pressure",
        "value": {
            "systolic": random.randint(90, 140),
            "diastolic": random.randint(60, 90)
        },
        "unit": "mmHg",
        "metadata": {
            "deviceId": f"iPhone14-{random.randint(1000, 9999)}",
            "appVersion": "1.2.3",
            "platform": "iOS"
        }
    }


def generate_test_dataset(num_users: int = 10, records_per_user: int = 10) -> List[Dict]:
    """
    Generate a complete test dataset
    
    Args:
        num_users: Number of unique users
        records_per_user: Number of records per user
    
    Returns:
        List of health data records
    """
    dataset = []
    base_time = datetime.utcnow()
    
    data_generators = [
        generate_heart_rate_data,
        generate_steps_data,
        generate_sleep_data,
        generate_blood_pressure_data,
    ]
    
    for user_idx in range(num_users):
        user_id = generate_user_id()
        
        for record_idx in range(records_per_user):
            # Generate timestamp with some offset
            timestamp = generate_timestamp(base_time, offset_seconds=record_idx * 60)
            
            # Randomly select data type
            generator = random.choice(data_generators)
            data = generator(user_id, timestamp)
            
            dataset.append(data)
    
    return dataset


def generate_invalid_test_data() -> List[Dict]:
    """Generate invalid test data for validation testing"""
    return [
        # Missing required fields
        {
            "userId": "550e8400-e29b-41d4-a716-446655440000",
            # Missing timestamp, dataType, value
        },
        # Invalid UUID
        {
            "userId": "not-a-valid-uuid",
            "timestamp": "2025-11-12T10:30:00Z",
            "dataType": "heart_rate",
            "value": 72.0,
        },
        # Invalid timestamp format
        {
            "userId": "550e8400-e29b-41d4-a716-446655440000",
            "timestamp": "invalid-timestamp",
            "dataType": "heart_rate",
            "value": 72.0,
        },
        # Invalid data type
        {
            "userId": "550e8400-e29b-41d4-a716-446655440000",
            "timestamp": "2025-11-12T10:30:00Z",
            "dataType": "unknown_type",
            "value": 72.0,
        },
        # Invalid value type
        {
            "userId": "550e8400-e29b-41d4-a716-446655440000",
            "timestamp": "2025-11-12T10:30:00Z",
            "dataType": "heart_rate",
            "value": "not-a-number",
        },
    ]


def save_test_data(filename: str, data: List[Dict]):
    """Save test data to JSON file"""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Saved {len(data)} records to {filename}")


if __name__ == "__main__":
    # Generate valid test data
    valid_data = generate_test_dataset(num_users=10, records_per_user=10)
    save_test_data("test_data_valid.json", valid_data)
    
    # Generate invalid test data
    invalid_data = generate_invalid_test_data()
    save_test_data("test_data_invalid.json", invalid_data)
    
    # Generate high-volume test data
    high_volume_data = generate_test_dataset(num_users=50, records_per_user=20)
    save_test_data("test_data_high_volume.json", high_volume_data)
    
    print("\nTest data generation complete!")
    print(f"- Valid data: {len(valid_data)} records")
    print(f"- Invalid data: {len(invalid_data)} records")
    print(f"- High volume data: {len(high_volume_data)} records")
