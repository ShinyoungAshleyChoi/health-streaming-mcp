"""
Test script for Iceberg catalog and table setup

This script demonstrates:
1. Connecting to Iceberg REST catalog
2. Creating namespace (database)
3. Creating tables with schemas and partitioning
4. Verifying table metadata
"""

import sys
import logging
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flink_consumer.config.settings import Settings
from flink_consumer.iceberg.catalog import IcebergCatalog
from flink_consumer.iceberg.table_manager import IcebergTableManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main test function"""
    logger.info("=" * 80)
    logger.info("Iceberg Catalog and Table Setup Test")
    logger.info("=" * 80)
    
    # Load settings
    logger.info("\n1. Loading configuration...")
    settings = Settings()
    logger.info(f"   Catalog Type: {settings.iceberg.catalog_type}")
    logger.info(f"   Catalog Name: {settings.iceberg.catalog_name}")
    logger.info(f"   Catalog URI: {settings.iceberg.catalog_uri}")
    logger.info(f"   Warehouse: {settings.iceberg.warehouse}")
    logger.info(f"   Database: {settings.iceberg.database}")
    logger.info(f"   S3 Endpoint: {settings.s3.endpoint}")
    
    # Initialize catalog
    logger.info("\n2. Initializing Iceberg REST catalog...")
    catalog = IcebergCatalog(settings)
    
    try:
        catalog.initialize()
        logger.info("   ✓ Catalog initialized successfully")
    except Exception as e:
        logger.error(f"   ✗ Failed to initialize catalog: {e}")
        return False
    
    # Test connection
    logger.info("\n3. Testing catalog connection...")
    if catalog.test_connection():
        logger.info("   ✓ Catalog connection successful")
    else:
        logger.error("   ✗ Catalog connection failed")
        return False
    
    # Initialize table manager
    logger.info("\n4. Initializing table manager...")
    table_manager = IcebergTableManager(settings, catalog)
    logger.info("   ✓ Table manager initialized")
    
    # Create namespace
    logger.info(f"\n5. Creating namespace: {settings.iceberg.database}")
    if table_manager.ensure_namespace_exists():
        logger.info("   ✓ Namespace ready")
    else:
        logger.error("   ✗ Failed to create namespace")
        return False
    
    # Create raw data table
    logger.info(f"\n6. Creating table: {settings.iceberg.table_raw}")
    raw_table = table_manager.create_health_data_raw_table()
    if raw_table:
        logger.info("   ✓ Raw data table created/verified")
        logger.info(f"   Location: {raw_table.location()}")
        logger.info(f"   Schema fields: {len(raw_table.schema().fields)}")
        logger.info(f"   Partition fields: {len(raw_table.spec().fields)}")
        
        # Display schema
        logger.info("\n   Schema:")
        for field in raw_table.schema().fields:
            required = "required" if field.required else "optional"
            logger.info(f"     - {field.name}: {field.field_type} ({required})")
        
        # Display partitioning
        logger.info("\n   Partitioning:")
        for field in raw_table.spec().fields:
            logger.info(f"     - {field.name}: {field.transform}")
    else:
        logger.error("   ✗ Failed to create raw data table")
        return False
    
    # Create errors table
    logger.info(f"\n7. Creating table: {settings.iceberg.table_errors}")
    errors_table = table_manager.create_health_data_errors_table()
    if errors_table:
        logger.info("   ✓ Errors table created/verified")
        logger.info(f"   Location: {errors_table.location()}")
        logger.info(f"   Schema fields: {len(errors_table.schema().fields)}")
        logger.info(f"   Partition fields: {len(errors_table.spec().fields)}")
    else:
        logger.error("   ✗ Failed to create errors table")
        return False
    
    # Verify all tables
    logger.info("\n8. Verifying all tables...")
    if table_manager.table_exists(settings.iceberg.table_raw):
        logger.info(f"   ✓ {settings.iceberg.table_raw} exists")
    else:
        logger.error(f"   ✗ {settings.iceberg.table_raw} not found")
        return False
    
    if table_manager.table_exists(settings.iceberg.table_errors):
        logger.info(f"   ✓ {settings.iceberg.table_errors} exists")
    else:
        logger.error(f"   ✗ {settings.iceberg.table_errors} not found")
        return False
    
    # Create aggregation tables
    logger.info("\n9. Creating aggregation tables...")
    if table_manager.initialize_all_aggregation_tables():
        logger.info("   ✓ All aggregation tables created/verified")
    else:
        logger.error("   ✗ Failed to create aggregation tables")
        return False
    
    # Verify aggregation tables
    logger.info("\n10. Verifying aggregation tables...")
    agg_tables = ["health_data_daily_agg", "health_data_weekly_agg", "health_data_monthly_agg"]
    for agg_table in agg_tables:
        if table_manager.table_exists(agg_table):
            logger.info(f"   ✓ {agg_table} exists")
            table = table_manager.get_table(agg_table)
            if table:
                logger.info(f"     Schema fields: {len(table.schema().fields)}")
                logger.info(f"     Partition fields: {len(table.spec().fields)}")
        else:
            logger.error(f"   ✗ {agg_table} not found")
            return False
    
    logger.info("\n" + "=" * 80)
    logger.info("✓ All tests passed successfully!")
    logger.info("  - Raw data table: health_data_raw")
    logger.info("  - Error table: health_data_errors")
    logger.info("  - Daily aggregation table: health_data_daily_agg")
    logger.info("  - Weekly aggregation table: health_data_weekly_agg")
    logger.info("  - Monthly aggregation table: health_data_monthly_agg")
    logger.info("=" * 80)
    
    return True


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\nUnexpected error: {e}", exc_info=True)
        sys.exit(1)
