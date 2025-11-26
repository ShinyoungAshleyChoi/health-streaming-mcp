"""
Example script to test Kafka source configuration and connectivity.

This script demonstrates how to:
1. Validate Kafka broker connectivity
2. Check topic existence and partitions
3. Test consumer group configuration
4. Create a basic Flink pipeline with Kafka source
"""

import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flink_consumer.config.settings import Settings
from flink_consumer.utils.kafka_utils import (
    KafkaConnectionTester,
    validate_kafka_config
)
from flink_consumer.services.kafka_source import KafkaSourceBuilder

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_kafka_connectivity():
    """Test basic Kafka connectivity and configuration."""
    logger.info("=" * 60)
    logger.info("Testing Kafka Connectivity")
    logger.info("=" * 60)
    
    # Load settings
    settings = Settings()
    
    # Create connection tester
    tester = KafkaConnectionTester(settings.kafka.brokers)
    
    # Test broker connection
    logger.info("\n1. Testing broker connection...")
    if not tester.test_broker_connection():
        logger.error("❌ Broker connection failed")
        return False
    logger.info("✅ Broker connection successful")
    
    # Check topic exists
    logger.info(f"\n2. Checking if topic '{settings.kafka.topic}' exists...")
    if not tester.check_topic_exists(settings.kafka.topic):
        logger.error(f"❌ Topic '{settings.kafka.topic}' not found")
        return False
    logger.info(f"✅ Topic '{settings.kafka.topic}' exists")
    
    # Get partition count
    logger.info(f"\n3. Getting partition information...")
    partition_count = tester.get_topic_partitions(settings.kafka.topic)
    if partition_count:
        logger.info(f"✅ Topic has {partition_count} partition(s)")
    else:
        logger.warning("⚠️  Could not retrieve partition information")
    
    # Test consumer connection
    logger.info(f"\n4. Testing consumer connection...")
    if not tester.test_consumer_connection(
        settings.kafka.topic,
        settings.kafka.group_id,
        settings.kafka.auto_offset_reset
    ):
        logger.error("❌ Consumer connection failed")
        return False
    logger.info("✅ Consumer connection successful")
    
    logger.info("\n" + "=" * 60)
    logger.info("All connectivity tests passed! ✅")
    logger.info("=" * 60)
    return True


def test_kafka_source_builder():
    """Test Kafka source builder configuration."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing Kafka Source Builder")
    logger.info("=" * 60)
    
    # Load settings
    settings = Settings()
    
    try:
        # Create Kafka source builder
        logger.info("\n1. Creating Kafka source builder...")
        builder = KafkaSourceBuilder(
            kafka_settings=settings.kafka,
            schema_registry_settings=settings.schema_registry
        )
        logger.info("✅ Kafka source builder created")
        
        # Build Kafka source
        logger.info("\n2. Building Kafka source...")
        kafka_source = builder.build()
        logger.info("✅ Kafka source built successfully")
        
        logger.info("\n" + "=" * 60)
        logger.info("Kafka source configuration:")
        logger.info(f"  - Brokers: {settings.kafka.brokers}")
        logger.info(f"  - Topic: {settings.kafka.topic}")
        logger.info(f"  - Group ID: {settings.kafka.group_id}")
        logger.info(f"  - Offset Reset: {settings.kafka.auto_offset_reset}")
        logger.info(f"  - Schema Registry: {settings.schema_registry.url}")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to build Kafka source: {e}", exc_info=True)
        return False


def test_schema_registry_connectivity():
    """Test Schema Registry connectivity."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing Schema Registry Connectivity")
    logger.info("=" * 60)
    
    from flink_consumer.converters.avro_deserializer import SchemaRegistryManager
    
    # Load settings
    settings = Settings()
    
    try:
        # Create Schema Registry manager
        logger.info("\n1. Connecting to Schema Registry...")
        manager = SchemaRegistryManager(settings.schema_registry.url)
        manager.connect()
        logger.info("✅ Schema Registry connection successful")
        
        # Try to get latest schema for the topic
        subject = f"{settings.kafka.topic}-value"
        logger.info(f"\n2. Fetching latest schema for subject '{subject}'...")
        schema_info = manager.get_latest_schema(subject)
        
        if schema_info:
            logger.info("✅ Schema retrieved successfully")
            logger.info(f"  - Schema ID: {schema_info['schema_id']}")
            logger.info(f"  - Version: {schema_info['version']}")
            logger.info(f"  - Subject: {schema_info['subject']}")
        else:
            logger.warning(f"⚠️  No schema found for subject '{subject}'")
            logger.info("This is expected if no messages have been produced yet")
        
        logger.info("\n" + "=" * 60)
        logger.info("Schema Registry test completed")
        logger.info("=" * 60)
        return True
        
    except Exception as e:
        logger.error(f"❌ Schema Registry test failed: {e}", exc_info=True)
        return False


def main():
    """Run all tests."""
    logger.info("\n" + "=" * 60)
    logger.info("Kafka Source Configuration Test Suite")
    logger.info("=" * 60)
    
    results = {
        "Kafka Connectivity": False,
        "Kafka Source Builder": False,
        "Schema Registry": False
    }
    
    # Run tests
    try:
        results["Kafka Connectivity"] = test_kafka_connectivity()
    except Exception as e:
        logger.error(f"Kafka connectivity test failed with exception: {e}")
    
    try:
        results["Kafka Source Builder"] = test_kafka_source_builder()
    except Exception as e:
        logger.error(f"Kafka source builder test failed with exception: {e}")
    
    try:
        results["Schema Registry"] = test_schema_registry_connectivity()
    except Exception as e:
        logger.error(f"Schema Registry test failed with exception: {e}")
    
    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("Test Summary")
    logger.info("=" * 60)
    for test_name, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        logger.info(f"{test_name}: {status}")
    
    all_passed = all(results.values())
    logger.info("=" * 60)
    if all_passed:
        logger.info("All tests passed! 🎉")
        return 0
    else:
        logger.error("Some tests failed. Please check the logs above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
