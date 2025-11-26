"""Load generator for performance testing"""

import time
import random
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any


class HealthDataLoadGenerator:
    """Generate health data payloads for load testing"""

    DATA_TYPES = [
        'heartRate',
        'steps',
        'distance',
        'activeEnergyBurned',
        'bloodPressureSystolic',
        'bloodGlucose',
        'oxygenSaturation',
        'bodyTemperature'
    ]

    UNITS = {
        'heartRate': 'count/min',
        'steps': 'count',
        'distance': 'm',
        'activeEnergyBurned': 'kcal',
        'bloodPressureSystolic': 'mmHg',
        'bloodGlucose': 'mg/dL',
        'oxygenSaturation': '%',
        'bodyTemperature': 'degC'
    }

    VALUE_RANGES = {
        'heartRate': (60, 100),
        'steps': (0, 10000),
        'distance': (0, 5000),
        'activeEnergyBurned': (0, 500),
        'bloodPressureSystolic': (90, 140),
        'bloodGlucose': (70, 120),
        'oxygenSaturation': (95, 100),
        'bodyTemperature': (36.0, 37.5)
    }

    def __init__(self, num_users: int = 100, num_devices: int = 50):
        """
        Initialize load generator.
        
        Args:
            num_users: Number of unique users to generate
            num_devices: Number of unique devices to generate
        """
        self.num_users = num_users
        self.num_devices = num_devices
        self.user_ids = [f'user-{i:06d}' for i in range(num_users)]
        self.device_ids = [f'device-{i:04d}' for i in range(num_devices)]

    def generate_payload(
        self,
        num_samples: int = 5,
        timestamp: datetime = None
    ) -> Dict[str, Any]:
        """
        Generate a single health data payload.
        
        Args:
            num_samples: Number of samples in the payload
            timestamp: Payload timestamp (defaults to now)
            
        Returns:
            Health data payload dictionary
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        user_id = random.choice(self.user_ids)
        device_id = random.choice(self.device_ids)

        samples = []
        for i in range(num_samples):
            data_type = random.choice(self.DATA_TYPES)
            sample = self._generate_sample(data_type, timestamp, i)
            samples.append(sample)

        payload = {
            'deviceId': device_id,
            'userId': user_id,
            'timestamp': timestamp.isoformat(),
            'appVersion': '1.0.0',
            'samples': samples
        }

        return payload

    def _generate_sample(
        self,
        data_type: str,
        base_timestamp: datetime,
        index: int
    ) -> Dict[str, Any]:
        """Generate a single health data sample"""
        # Generate timestamps
        start_offset = timedelta(minutes=index * 5)
        end_offset = timedelta(minutes=index * 5 + 1)
        
        start_date = base_timestamp + start_offset
        end_date = base_timestamp + end_offset
        created_at = end_date + timedelta(seconds=1)

        # Generate value within range
        value_range = self.VALUE_RANGES.get(data_type, (0, 100))
        if data_type == 'bodyTemperature':
            value = round(random.uniform(*value_range), 1)
        else:
            value = round(random.uniform(*value_range), 2)

        sample = {
            'id': f'sample-{int(time.time() * 1000)}-{index}',
            'type': data_type,
            'value': value,
            'unit': self.UNITS.get(data_type, 'count'),
            'startDate': start_date.isoformat(),
            'endDate': end_date.isoformat(),
            'sourceBundle': 'com.apple.health',
            'metadata': {},
            'isSynced': random.choice([True, False]),
            'createdAt': created_at.isoformat()
        }

        return sample

    def generate_batch(
        self,
        batch_size: int,
        samples_per_payload: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Generate a batch of payloads.
        
        Args:
            batch_size: Number of payloads to generate
            samples_per_payload: Number of samples per payload
            
        Returns:
            List of health data payloads
        """
        payloads = []
        base_timestamp = datetime.now(timezone.utc)

        for i in range(batch_size):
            # Vary timestamp slightly for each payload
            timestamp = base_timestamp + timedelta(seconds=i)
            payload = self.generate_payload(samples_per_payload, timestamp)
            payloads.append(payload)

        return payloads

    def generate_continuous_stream(
        self,
        duration_seconds: int,
        rate_per_second: int,
        samples_per_payload: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Generate payloads for continuous stream simulation.
        
        Args:
            duration_seconds: Duration of the stream in seconds
            rate_per_second: Number of payloads per second
            samples_per_payload: Number of samples per payload
            
        Returns:
            List of health data payloads with timestamps spread over duration
        """
        total_payloads = duration_seconds * rate_per_second
        payloads = []
        
        base_timestamp = datetime.now(timezone.utc)
        
        for i in range(total_payloads):
            # Spread timestamps evenly over duration
            offset_seconds = (i / rate_per_second)
            timestamp = base_timestamp + timedelta(seconds=offset_seconds)
            
            payload = self.generate_payload(samples_per_payload, timestamp)
            payloads.append(payload)

        return payloads


def generate_test_data(
    num_payloads: int = 1000,
    samples_per_payload: int = 5,
    num_users: int = 100
) -> List[Dict[str, Any]]:
    """
    Convenience function to generate test data.
    
    Args:
        num_payloads: Number of payloads to generate
        samples_per_payload: Number of samples per payload
        num_users: Number of unique users
        
    Returns:
        List of health data payloads
    """
    generator = HealthDataLoadGenerator(num_users=num_users)
    return generator.generate_batch(num_payloads, samples_per_payload)


if __name__ == '__main__':
    # Example usage
    print("Generating sample health data...")
    
    generator = HealthDataLoadGenerator(num_users=10, num_devices=5)
    
    # Generate single payload
    payload = generator.generate_payload(num_samples=3)
    print(f"\nSample payload with {len(payload['samples'])} samples:")
    print(f"  User: {payload['userId']}")
    print(f"  Device: {payload['deviceId']}")
    print(f"  Timestamp: {payload['timestamp']}")
    
    # Generate batch
    batch = generator.generate_batch(batch_size=100, samples_per_payload=5)
    print(f"\nGenerated batch of {len(batch)} payloads")
    print(f"Total samples: {sum(len(p['samples']) for p in batch)}")
    
    # Generate continuous stream
    stream = generator.generate_continuous_stream(
        duration_seconds=60,
        rate_per_second=10,
        samples_per_payload=5
    )
    print(f"\nGenerated stream of {len(stream)} payloads over 60 seconds")
    print(f"Rate: {len(stream) / 60:.1f} payloads/second")
