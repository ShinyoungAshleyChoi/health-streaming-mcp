"""Example script demonstrating schema evolution functionality"""

import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from converters.schema_evolution import (
    SchemaCompatibilityChecker,
    CompatibilityType,
    SchemaChangeType,
)
from iceberg.schema_evolution import (
    IcebergSchemaEvolutionManager,
    SchemaOperation,
    SchemaOperationType,
    get_iceberg_type_from_string,
)
from pyiceberg.types import StringType, DoubleType, LongType
from config.logging import setup_logging

# Setup logging
setup_logging(log_level="INFO", log_format="text")
logger = logging.getLogger(__name__)


def test_avro_schema_compatibility():
    """Test Avro schema compatibility checking with Schema Registry"""
    logger.info("\n" + "=" * 80)
    logger.info("Testing Avro Schema Compatibility Checking")
    logger.info("=" * 80)
    
    # Initialize checker
    schema_registry_url = "http://localhost:8081"
    checker = SchemaCompatibilityChecker(schema_registry_url)
    
    try:
        # Connect to Schema Registry
        checker.connect()
        logger.info("✓ Connected to Schema Registry")
        
        # Get latest schema for health-data-raw topic
        subject = "health-data-raw-value"
        latest_schema = checker.get_latest_schema(subject)
        
        if latest_schema:
            logger.info(f"✓ Retrieved latest schema for '{subject}':")
            logger.info(f"  - Version: {latest_schema.version}")
            logger.info(f"  - Schema ID: {latest_schema.schema_id}")
        else:
            logger.warning(f"✗ No schema found for subject '{subject}'")
            logger.info("  This is expected if the topic hasn't been used yet")
            return
        
        # Validate schema evolution
        is_valid, changes = checker.validate_schema_evolution(subject)
        
        if is_valid:
            logger.info(f"✓ Schema evolution validation passed")
            
            if changes:
                logger.info(f"  Detected {len(changes)} change(s):")
                for change in changes:
                    status = "⚠ BREAKING" if change.is_breaking else "✓ Compatible"
                    logger.info(f"    {status}: {change.description}")
            else:
                logger.info("  No changes detected (first version or no evolution)")
        else:
            logger.error(f"✗ Schema evolution validation failed")
            for change in changes:
                if change.is_breaking:
                    logger.error(f"  - {change.description}")
        
        # Example: Check compatibility of a new schema
        logger.info("\nTesting schema compatibility check...")
        
        # This is a sample new schema with an added optional field
        new_schema_str = '''{
            "type": "record",
            "name": "HealthData",
            "fields": [
                {"name": "deviceId", "type": "string"},
                {"name": "userId", "type": "string"},
                {"name": "timestamp", "type": "string"},
                {"name": "appVersion", "type": "string"},
                {"name": "newOptionalField", "type": ["null", "string"], "default": null}
            ]
        }'''
        
        is_compatible, errors = checker.check_compatibility(
            subject, 
            new_schema_str,
            CompatibilityType.BACKWARD
        )
        
        if is_compatible:
            logger.info("✓ New schema is compatible")
        else:
            logger.warning("✗ New schema is not compatible:")
            for error in errors:
                logger.warning(f"  - {error}")
        
    except Exception as e:
        logger.error(f"✗ Error during schema compatibility testing: {e}")
        logger.info("  Make sure Schema Registry is running at http://localhost:8081")


def test_iceberg_schema_evolution():
    """Test Iceberg schema evolution utilities"""
    logger.info("\n" + "=" * 80)
    logger.info("Testing Iceberg Schema Evolution")
    logger.info("=" * 80)
    
    # Initialize manager
    manager = IcebergSchemaEvolutionManager(
        catalog_name="health_catalog",
        catalog_type="hadoop",
        warehouse="s3a://data-lake/warehouse"
    )
    
    try:
        # Connect to catalog
        manager.connect()
        logger.info("✓ Connected to Iceberg catalog")
        
        database = "health_db"
        table_name = "health_data_raw"
        
        # Get current schema
        current_schema = manager.get_current_schema(database, table_name)
        
        if current_schema:
            logger.info(f"✓ Retrieved current schema for '{database}.{table_name}':")
            logger.info(f"  - Schema ID: {current_schema.schema_id}")
            logger.info(f"  - Fields: {len(current_schema.fields)}")
            
            for field in current_schema.fields[:5]:  # Show first 5 fields
                logger.info(f"    - {field.name}: {field.field_type} (required={field.required})")
            
            if len(current_schema.fields) > 5:
                logger.info(f"    ... and {len(current_schema.fields) - 5} more fields")
        else:
            logger.warning(f"✗ Table '{database}.{table_name}' not found")
            logger.info("  This is expected if the table hasn't been created yet")
            return
        
        # Example 1: Add optional column
        logger.info("\nExample 1: Adding optional column 'device_model'")
        
        operation = SchemaOperation(
            operation_type=SchemaOperationType.ADD_COLUMN,
            column_name="device_model",
            column_type="string",
            required=False,
            doc="Device model information"
        )
        
        is_valid, error = manager.validate_schema_change(database, table_name, operation)
        
        if is_valid:
            logger.info("✓ Schema change validation passed")
            
            # Uncomment to actually apply the change
            # success = manager.add_column(
            #     database=database,
            #     table_name=table_name,
            #     column_name="device_model",
            #     column_type=StringType(),
            #     required=False,
            #     doc="Device model information"
            # )
            # 
            # if success:
            #     logger.info("✓ Column added successfully")
            # else:
            #     logger.error("✗ Failed to add column")
            
            logger.info("  (Skipping actual schema change - uncomment to apply)")
        else:
            logger.error(f"✗ Schema change validation failed: {error}")
        
        # Example 2: Rename column (demonstration only)
        logger.info("\nExample 2: Renaming column (demonstration)")
        
        operation = SchemaOperation(
            operation_type=SchemaOperationType.RENAME_COLUMN,
            column_name="user_id",
            new_column_name="patient_id"
        )
        
        is_valid, error = manager.validate_schema_change(database, table_name, operation)
        
        if is_valid:
            logger.info("✓ Rename operation would be valid")
            logger.info("  (Not applying - this is just a demonstration)")
        else:
            logger.error(f"✗ Rename operation validation failed: {error}")
        
        # Example 3: Update column type (int -> long)
        logger.info("\nExample 3: Type widening (demonstration)")
        logger.info("  Compatible type promotions:")
        logger.info("    - int -> long")
        logger.info("    - float -> double")
        logger.info("  These are safe schema evolutions")
        
        # Show schema history
        logger.info("\nSchema Evolution History:")
        manager.print_schema_history(database, table_name)
        
    except Exception as e:
        logger.error(f"✗ Error during Iceberg schema evolution testing: {e}")
        logger.info("  Make sure Iceberg catalog is properly configured")


def test_schema_change_scenarios():
    """Test various schema change scenarios"""
    logger.info("\n" + "=" * 80)
    logger.info("Testing Schema Change Scenarios")
    logger.info("=" * 80)
    
    logger.info("\n1. Safe Schema Changes (Backward Compatible):")
    logger.info("   ✓ Add optional field with default value")
    logger.info("   ✓ Make required field optional")
    logger.info("   ✓ Widen numeric type (int -> long, float -> double)")
    logger.info("   ✓ Add new enum value")
    
    logger.info("\n2. Breaking Schema Changes (Not Backward Compatible):")
    logger.info("   ✗ Add required field without default value")
    logger.info("   ✗ Remove existing field")
    logger.info("   ✗ Change field type (incompatible)")
    logger.info("   ✗ Rename field (without alias)")
    
    logger.info("\n3. Best Practices:")
    logger.info("   • Always add optional fields with default values")
    logger.info("   • Use schema evolution for gradual rollout")
    logger.info("   • Test schema changes in development first")
    logger.info("   • Document schema changes in version control")
    logger.info("   • Monitor Schema Registry compatibility settings")


def main():
    """Main function to run all tests"""
    logger.info("=" * 80)
    logger.info("Schema Evolution Testing Suite")
    logger.info("=" * 80)
    
    # Test 1: Avro schema compatibility with Schema Registry
    test_avro_schema_compatibility()
    
    # Test 2: Iceberg schema evolution
    test_iceberg_schema_evolution()
    
    # Test 3: Schema change scenarios
    test_schema_change_scenarios()
    
    logger.info("\n" + "=" * 80)
    logger.info("Schema Evolution Testing Complete")
    logger.info("=" * 80)
    logger.info("\nNote: Some tests are demonstrations and don't apply actual changes.")
    logger.info("Uncomment the relevant code sections to apply real schema changes.")


if __name__ == "__main__":
    main()
