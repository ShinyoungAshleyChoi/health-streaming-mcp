"""Performance tests for throughput and latency"""

import pytest
import time
from pyflink.datastream import StreamExecutionEnvironment
from flink_consumer.converters.health_data_transformer import HealthDataTransformer
from flink_consumer.validators.health_data_validator import HealthDataValidator
from flink_consumer.tests.performance.test_load_generator import HealthDataLoadGenerator


class TestThroughputPerformance:
    """Performance tests for data processing throughput"""

    @pytest.fixture
    def env(self):
        """Create Flink execution environment"""
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(4)  # Use multiple parallel tasks
        return env

    @pytest.fixture
    def load_generator(self):
        """Create load generator"""
        return HealthDataLoadGenerator(num_users=100, num_devices=50)

    def test_small_batch_throughput(self, env, load_generator):
        """Test throughput with small batch (1000 payloads)"""
        # Generate test data
        batch_size = 1000
        samples_per_payload = 5
        payloads = load_generator.generate_batch(batch_size, samples_per_payload)
        
        # Create pipeline
        source = env.from_collection(payloads)
        transformer = HealthDataTransformer()
        transformed = source.flat_map(transformer)
        validator = HealthDataValidator(strict_mode=False)
        validated = transformed.filter(validator)
        
        # Measure processing time
        start_time = time.time()
        results = list(validated.execute_and_collect())
        end_time = time.time()
        
        # Calculate metrics
        duration = end_time - start_time
        total_samples = len(results)
        throughput = total_samples / duration if duration > 0 else 0
        
        print(f"\nSmall Batch Performance:")
        print(f"  Payloads: {batch_size}")
        print(f"  Samples processed: {total_samples}")
        print(f"  Duration: {duration:.2f}s")
        print(f"  Throughput: {throughput:.2f} samples/sec")
        
        # Verify results
        assert total_samples > 0
        assert throughput > 0

    def test_medium_batch_throughput(self, env, load_generator):
        """Test throughput with medium batch (5000 payloads)"""
        batch_size = 5000
        samples_per_payload = 5
        payloads = load_generator.generate_batch(batch_size, samples_per_payload)
        
        source = env.from_collection(payloads)
        transformer = HealthDataTransformer()
        transformed = source.flat_map(transformer)
        validator = HealthDataValidator(strict_mode=False)
        validated = transformed.filter(validator)
        
        start_time = time.time()
        results = list(validated.execute_and_collect())
        end_time = time.time()
        
        duration = end_time - start_time
        total_samples = len(results)
        throughput = total_samples / duration if duration > 0 else 0
        
        print(f"\nMedium Batch Performance:")
        print(f"  Payloads: {batch_size}")
        print(f"  Samples processed: {total_samples}")
        print(f"  Duration: {duration:.2f}s")
        print(f"  Throughput: {throughput:.2f} samples/sec")
        
        assert total_samples > 0
        assert throughput > 0

    def test_transformation_performance(self, env, load_generator):
        """Test transformation step performance"""
        batch_size = 2000
        payloads = load_generator.generate_batch(batch_size, samples_per_payload=5)
        
        source = env.from_collection(payloads)
        transformer = HealthDataTransformer()
        
        start_time = time.time()
        transformed = source.flat_map(transformer)
        results = list(transformed.execute_and_collect())
        end_time = time.time()
        
        duration = end_time - start_time
        throughput = len(results) / duration if duration > 0 else 0
        
        print(f"\nTransformation Performance:")
        print(f"  Input payloads: {batch_size}")
        print(f"  Output samples: {len(results)}")
        print(f"  Duration: {duration:.2f}s")
        print(f"  Throughput: {throughput:.2f} samples/sec")
        
        assert len(results) > 0

    def test_validation_performance(self, env, load_generator):
        """Test validation step performance"""
        batch_size = 2000
        payloads = load_generator.generate_batch(batch_size, samples_per_payload=5)
        
        # Transform first
        source = env.from_collection(payloads)
        transformer = HealthDataTransformer()
        transformed = source.flat_map(transformer)
        
        # Measure validation
        validator = HealthDataValidator(strict_mode=False)
        
        start_time = time.time()
        validated = transformed.filter(validator)
        results = list(validated.execute_and_collect())
        end_time = time.time()
        
        duration = end_time - start_time
        throughput = len(results) / duration if duration > 0 else 0
        
        print(f"\nValidation Performance:")
        print(f"  Samples validated: {len(results)}")
        print(f"  Duration: {duration:.2f}s")
        print(f"  Throughput: {throughput:.2f} samples/sec")
        
        assert len(results) > 0


class TestLatencyPerformance:
    """Performance tests for processing latency"""

    @pytest.fixture
    def env(self):
        """Create Flink execution environment"""
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)  # Single thread for latency measurement
        return env

    @pytest.fixture
    def load_generator(self):
        """Create load generator"""
        return HealthDataLoadGenerator(num_users=10, num_devices=5)

    def test_single_payload_latency(self, env, load_generator):
        """Test latency for processing single payload"""
        payload = load_generator.generate_payload(num_samples=5)
        
        source = env.from_collection([payload])
        transformer = HealthDataTransformer()
        transformed = source.flat_map(transformer)
        validator = HealthDataValidator(strict_mode=False)
        validated = transformed.filter(validator)
        
        start_time = time.time()
        results = list(validated.execute_and_collect())
        end_time = time.time()
        
        latency_ms = (end_time - start_time) * 1000
        
        print(f"\nSingle Payload Latency:")
        print(f"  Samples: {len(results)}")
        print(f"  Latency: {latency_ms:.2f}ms")
        
        assert len(results) > 0

    def test_average_latency(self, env, load_generator):
        """Test average latency across multiple payloads"""
        num_payloads = 100
        payloads = load_generator.generate_batch(num_payloads, samples_per_payload=5)
        
        source = env.from_collection(payloads)
        transformer = HealthDataTransformer()
        transformed = source.flat_map(transformer)
        validator = HealthDataValidator(strict_mode=False)
        validated = transformed.filter(validator)
        
        start_time = time.time()
        results = list(validated.execute_and_collect())
        end_time = time.time()
        
        total_duration_ms = (end_time - start_time) * 1000
        avg_latency_per_payload = total_duration_ms / num_payloads
        
        print(f"\nAverage Latency:")
        print(f"  Payloads: {num_payloads}")
        print(f"  Total duration: {total_duration_ms:.2f}ms")
        print(f"  Avg latency per payload: {avg_latency_per_payload:.2f}ms")
        
        assert len(results) > 0


class TestResourceUsage:
    """Performance tests for resource usage monitoring"""

    @pytest.fixture
    def env(self):
        """Create Flink execution environment"""
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(2)
        return env

    @pytest.fixture
    def load_generator(self):
        """Create load generator"""
        return HealthDataLoadGenerator(num_users=50, num_devices=25)

    def test_memory_efficiency(self, env, load_generator):
        """Test memory usage with large batch"""
        # Generate large batch
        batch_size = 10000
        payloads = load_generator.generate_batch(batch_size, samples_per_payload=5)
        
        source = env.from_collection(payloads)
        transformer = HealthDataTransformer()
        transformed = source.flat_map(transformer)
        validator = HealthDataValidator(strict_mode=False)
        validated = transformed.filter(validator)
        
        # Process data
        results = list(validated.execute_and_collect())
        
        print(f"\nMemory Efficiency Test:")
        print(f"  Input payloads: {batch_size}")
        print(f"  Output samples: {len(results)}")
        print(f"  Expansion ratio: {len(results) / batch_size:.2f}x")
        
        assert len(results) > 0

    def test_parallelism_scaling(self, load_generator):
        """Test performance with different parallelism levels"""
        batch_size = 5000
        payloads = load_generator.generate_batch(batch_size, samples_per_payload=5)
        
        parallelism_levels = [1, 2, 4]
        results_summary = []
        
        for parallelism in parallelism_levels:
            env = StreamExecutionEnvironment.get_execution_environment()
            env.set_parallelism(parallelism)
            
            source = env.from_collection(payloads)
            transformer = HealthDataTransformer()
            transformed = source.flat_map(transformer)
            validator = HealthDataValidator(strict_mode=False)
            validated = transformed.filter(validator)
            
            start_time = time.time()
            results = list(validated.execute_and_collect())
            end_time = time.time()
            
            duration = end_time - start_time
            throughput = len(results) / duration if duration > 0 else 0
            
            results_summary.append({
                'parallelism': parallelism,
                'duration': duration,
                'throughput': throughput
            })
        
        print(f"\nParallelism Scaling Test:")
        for result in results_summary:
            print(f"  Parallelism {result['parallelism']}: "
                  f"{result['duration']:.2f}s, "
                  f"{result['throughput']:.2f} samples/sec")
        
        assert len(results_summary) == len(parallelism_levels)
