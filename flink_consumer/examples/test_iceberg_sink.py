"""
Example script to test Iceberg sink implementation

This script demonstrates:
1. PyFlink Table API integration with Iceberg catalog
2. Batch writing strategy with buffering
3. Error table sink for DLQ pattern
"""

import logging
import sys
from datetime import datetime, timezone
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_iceberg_sink_initialization():
    """
    Test 1: Initialize Iceberg sink and register catalog
    
    Tests:
    - StreamTableEnvironment creation
    - Iceberg catalog registration
    - Database creation
    """
    try:
        from pyflink.datastream import StreamExecutionEnvironment
        from flink_consumer.config.settings import Settings
        from flink_consumer.iceberg.sink import IcebergSink
        
        logger.info("=" * 80)
        logger.info("Test 1: Iceberg Sink Initialization")
        logger.info("=" * 80)
        
        # Create execution environment
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)
        
        # Load settings
        settings = Settings()
        
        # Create Iceberg sink
        iceberg_sink = IcebergSink(settings, env)
        
        # Initialize sink (register catalog and create database)
        success = iceberg_sink.initialize()
        
        if success:
            logger.info("✓ Iceberg sink initialized successfully")
            logger.info(f"  - Catalog: {settings.iceberg.catalog_name}")
            logger.info(f"  - Database: {settings.iceberg.database}")
            logger.info(f"  - Warehouse: {settings.iceberg.warehouse}")
        else:
            logger.error("✗ Failed to initialize Iceberg sink")
            return False
        
        # Get table environment
        table_env = iceberg_sink.get_table_env()
        logger.info(f"✓ StreamTableEnvironment created: {type(table_env)}")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Test failed: {str(e)}", exc_info=True)
        return False


def test_batch_sink_configuration():
    """
    Test 2: Test batch sink configuration
    
    Tests:
    - Batch writing configuration
    - Buffering strategy settings
    - File size optimization
    """
    try:
        from pyflink.datastream import StreamExecutionEnvironment
        from flink_consumer.config.settings import Settings
        from flink_consumer.iceberg.sink import IcebergSink
        
        logger.info("=" * 80)
        logger.info("Test 2: Batch Sink Configuration")
        logger.info("=" * 80)
        
        # Create execution environment
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)
        
        # Load settings
        settings = Settings()
        
        # Create Iceberg sink
        iceberg_sink = IcebergSink(settings, env)
        iceberg_sink.initialize()
        
        # Create batch sink configuration
        batch_config = iceberg_sink.create_batch_sink(
            table_name=settings.iceberg.table_raw
        )
        
        if batch_config:
            logger.info("✓ Batch sink configuration created")
            logger.info(f"  - Buffer size: {settings.batch.batch_size} records")
            logger.info(f"  - Buffer timeout: {settings.batch.batch_timeout_seconds} seconds")
            logger.info(f"  - Target file size: {settings.batch.target_file_size_mb} MB")
            logger.info(f"  - Compression: snappy")
            
            # Display configuration
            logger.info("  Configuration details:")
            for key, value in batch_config.items():
                logger.info(f"    {key}: {value}")
        else:
            logger.error("✗ Failed to create batch sink configuration")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Test failed: {str(e)}", exc_info=True)
        return False


def test_error_sink():
    """
    Test 3: Test error sink for DLQ pattern
    
    Tests:
    - Error record creation
    - Error sink initialization
    - Error metrics logging
    """
    try:
        from pyflink.datastream import StreamExecutionEnvironment
        from flink_consumer.config.settings import Settings
        from flink_consumer.iceberg.sink import IcebergSink, IcebergErrorSink
        
        logger.info("=" * 80)
        logger.info("Test 3: Error Sink (DLQ)")
        logger.info("=" * 80)
        
        # Create execution environment
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)
        
        # Load settings
        settings = Settings()
        
        # Create main Iceberg sink
        iceberg_sink = IcebergSink(settings, env)
        iceberg_sink.initialize()
        
        # Create error sink
        error_sink = IcebergErrorSink(settings, iceberg_sink)
        
        # Create sample error record
        error_record = error_sink.create_error_record(
            error_type="validation_error",
            error_message="Missing required field: user_id",
            raw_payload='{"deviceId": "test-123", "samples": []}',
            device_id="test-123",
            kafka_topic="health-data-raw",
            kafka_partition=0,
            kafka_offset=12345
        )
        
        logger.info("✓ Error record created")
        logger.info(f"  - Error ID: {error_record['error_id']}")
        logger.info(f"  - Error Type: {error_record['error_type']}")
        logger.info(f"  - Error Message: {error_record['error_message']}")
        logger.info(f"  - Kafka Topic: {error_record['kafka_topic']}")
        logger.info(f"  - Kafka Partition: {error_record['kafka_partition']}")
        logger.info(f"  - Kafka Offset: {error_record['kafka_offset']}")
        
        # Log error metrics
        error_sink.log_error_metrics("validation_error", count=1)
        logger.info("✓ Error metrics logged")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Test failed: {str(e)}", exc_info=True)
        return False


def test_datastream_to_table_conversion():
    """
    Test 4: Test DataStream to Table conversion
    
    Tests:
    - Creating sample DataStream
    - Converting to Table
    - Schema validation
    """
    try:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.common import Row
        from flink_consumer.config.settings import Settings
        from flink_consumer.iceberg.sink import IcebergSink
        
        logger.info("=" * 80)
        logger.info("Test 4: DataStream to Table Conversion")
        logger.info("=" * 80)
        
        # Create execution environment
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)
        
        # Load settings
        settings = Settings()
        
        # Create Iceberg sink
        iceberg_sink = IcebergSink(settings, env)
        iceberg_sink.initialize()
        
        # Create sample data
        sample_data = [
            Row(
                device_id="device-001",
                user_id="user-001",
                sample_id="sample-001",
                data_type="heartRate",
                value=72.5,
                unit="count/min",
                start_date=datetime.now(timezone.utc),
                end_date=datetime.now(timezone.utc),
                source_bundle=None,
                metadata={},
                is_synced=True,
                created_at=datetime.now(timezone.utc),
                payload_timestamp=datetime.now(timezone.utc),
                app_version="1.0.0",
                processing_time=datetime.now(timezone.utc)
            )
        ]
        
        # Create DataStream from collection
        data_stream = env.from_collection(sample_data)
        
        logger.info("✓ Sample DataStream created")
        logger.info(f"  - Records: {len(sample_data)}")
        
        # Define schema fields
        schema_fields = [
            "device_id", "user_id", "sample_id", "data_type", "value",
            "unit", "start_date", "end_date", "source_bundle", "metadata",
            "is_synced", "created_at", "payload_timestamp", "app_version",
            "processing_time"
        ]
        
        # Convert to Table
        table = iceberg_sink.datastream_to_table(data_stream, schema_fields)
        
        if table:
            logger.info("✓ DataStream converted to Table successfully")
            logger.info(f"  - Schema fields: {len(schema_fields)}")
        else:
            logger.error("✗ Failed to convert DataStream to Table")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Test failed: {str(e)}", exc_info=True)
        return False


def run_all_tests():
    """Run all Iceberg sink tests"""
    logger.info("\n" + "=" * 80)
    logger.info("ICEBERG SINK TEST SUITE")
    logger.info("=" * 80 + "\n")
    
    tests = [
        ("Sink Initialization", test_iceberg_sink_initialization),
        ("Batch Configuration", test_batch_sink_configuration),
        ("Error Sink (DLQ)", test_error_sink),
        ("DataStream Conversion", test_datastream_to_table_conversion),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
            logger.info("")
        except Exception as e:
            logger.error(f"Test '{test_name}' crashed: {str(e)}", exc_info=True)
            results.append((test_name, False))
            logger.info("")
    
    # Print summary
    logger.info("=" * 80)
    logger.info("TEST SUMMARY")
    logger.info("=" * 80)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASSED" if result else "✗ FAILED"
        logger.info(f"{status}: {test_name}")
    
    logger.info("=" * 80)
    logger.info(f"Results: {passed}/{total} tests passed")
    logger.info("=" * 80)
    
    return passed == total


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
