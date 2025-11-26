"""
Example: Time-based aggregation pipeline for health data.

This example demonstrates how to set up and run the complete aggregation
pipeline with daily, weekly, and monthly aggregations.
"""

import logging
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_sample_health_data():
    """
    Create sample health data for testing.
    
    Returns:
        List of health data records
    """
    base_time = int(datetime(2025, 11, 17, 10, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    
    records = []
    for i in range(100):
        record = {
            'device_id': f'device-{i % 5}',
            'user_id': f'user-{i % 10}',
            'sample_id': f'sample-{i}',
            'data_type': 'heartRate' if i % 2 == 0 else 'steps',
            'value': 72.0 + (i % 20) if i % 2 == 0 else 5000.0 + (i * 100),
            'unit': 'count/min' if i % 2 == 0 else 'count',
            'start_date': base_time + (i * 60000),  # 1 minute apart
            'end_date': base_time + (i * 60000) + 60000,
            'source_bundle': 'com.example.health',
            'metadata': {},
            'is_synced': True,
            'created_at': base_time + (i * 60000),
            'payload_timestamp': base_time,
            'app_version': '1.0.0',
            'processing_time': base_time + (i * 60000),
        }
        records.append(record)
    
    return records


def example_basic_aggregation_pipeline():
    """
    Example: Basic aggregation pipeline setup.
    
    This example shows how to create a simple aggregation pipeline
    with default configuration.
    """
    logger.info("=== Example: Basic Aggregation Pipeline ===")
    
    try:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.table import StreamTableEnvironment, EnvironmentSettings
        from flink_consumer.aggregations import (
            create_aggregation_pipeline,
            AggregationPipelineConfig,
            AggregationSinkConfig,
        )
        
        # Create execution environment
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(4)
        
        # Create table environment
        settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        table_env = StreamTableEnvironment.create(env, settings)
        
        # Create sample data stream
        sample_data = create_sample_health_data()
        raw_stream = env.from_collection(sample_data)
        
        # Configure aggregation pipeline
        pipeline_config = AggregationPipelineConfig(
            enable_daily=True,
            enable_weekly=True,
            enable_monthly=True,
            daily_parallelism=12,
            weekly_parallelism=6,
            monthly_parallelism=3,
        )
        
        # Configure sink
        sink_config = AggregationSinkConfig(
            catalog_name='health_catalog',
            database_name='health_db',
            enable_upsert=True,
        )
        
        # Create aggregation pipeline
        daily, weekly, monthly = create_aggregation_pipeline(
            env, table_env, raw_stream,
            pipeline_config, sink_config
        )
        
        logger.info("Aggregation pipeline created successfully")
        logger.info(f"Daily stream: {daily is not None}")
        logger.info(f"Weekly stream: {weekly is not None}")
        logger.info(f"Monthly stream: {monthly is not None}")
        
        # Execute (in production, this would run continuously)
        # env.execute("Health Data Aggregation Pipeline")
        
    except ImportError as e:
        logger.warning(f"PyFlink not available, skipping example: {e}")


def example_custom_window_configuration():
    """
    Example: Custom window configuration.
    
    This example shows how to configure windows with custom settings.
    """
    logger.info("=== Example: Custom Window Configuration ===")
    
    from flink_consumer.aggregations import (
        WindowType,
        WindowConfig,
        create_tumbling_window,
    )
    
    # Get daily window configuration
    daily_config = WindowConfig.get_config(WindowType.DAILY)
    logger.info(f"Daily window: {daily_config}")
    
    # Create tumbling window configuration
    window_config = create_tumbling_window(WindowType.WEEKLY)
    logger.info(f"Weekly window config: {window_config}")
    
    # Window alignment example
    from flink_consumer.aggregations.windows import WindowAlignmentHelper
    
    timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    
    midnight = WindowAlignmentHelper.align_to_midnight_utc(timestamp_ms)
    monday = WindowAlignmentHelper.align_to_monday(timestamp_ms)
    first_of_month = WindowAlignmentHelper.align_to_first_of_month(timestamp_ms)
    
    logger.info(f"Aligned to midnight: {datetime.fromtimestamp(midnight/1000, tz=timezone.utc)}")
    logger.info(f"Aligned to Monday: {datetime.fromtimestamp(monday/1000, tz=timezone.utc)}")
    logger.info(f"Aligned to 1st: {datetime.fromtimestamp(first_of_month/1000, tz=timezone.utc)}")


def example_aggregation_validation():
    """
    Example: Aggregation validation.
    
    This example shows how to validate aggregation results.
    """
    logger.info("=== Example: Aggregation Validation ===")
    
    from flink_consumer.aggregations import (
        AggregationValidator,
        DataQualityChecker,
    )
    
    # Create validator
    validator = AggregationValidator(enable_anomaly_detection=True)
    
    # Sample aggregation result
    valid_agg = {
        'user_id': 'user-123',
        'data_type': 'heartRate',
        'count': 100,
        'min_value': 60.0,
        'max_value': 90.0,
        'avg_value': 75.0,
        'sum_value': 7500.0,
        'stddev_value': 8.5,
        'record_count': 100,
        'window_start': int(datetime.now(timezone.utc).timestamp() * 1000),
        'window_end': int(datetime.now(timezone.utc).timestamp() * 1000) + 86400000,
        'updated_at': int(datetime.now(timezone.utc).timestamp() * 1000),
    }
    
    # Validate
    is_valid = validator.filter(valid_agg)
    logger.info(f"Validation result: {is_valid}")
    logger.info(f"Has anomaly: {valid_agg.get('has_anomaly', False)}")
    
    # Check data quality
    completeness = DataQualityChecker.check_completeness(valid_agg)
    logger.info(f"Completeness: {sum(completeness.values())}/{len(completeness)} fields")
    
    quality_score = DataQualityChecker.calculate_quality_score(valid_agg)
    logger.info(f"Quality score: {quality_score:.2f}")
    
    # Invalid aggregation example
    invalid_agg = {
        'user_id': 'user-456',
        'data_type': 'heartRate',
        'count': 0,  # Invalid: zero count
        'min_value': 100.0,
        'max_value': 50.0,  # Invalid: min > max
        'avg_value': 75.0,
    }
    
    is_valid = validator.filter(invalid_agg)
    logger.info(f"Invalid aggregation validation: {is_valid}")
    
    # Get validation stats
    stats = validator.get_validation_stats()
    logger.info(f"Validation stats: {stats}")


def example_metrics_collection():
    """
    Example: Metrics collection and reporting.
    
    This example shows how to collect and report aggregation metrics.
    """
    logger.info("=== Example: Metrics Collection ===")
    
    from flink_consumer.aggregations import (
        WindowType,
        MetricsCollector,
        setup_metrics_collection,
    )
    
    # Setup metrics collection
    collector = setup_metrics_collection(
        enable_prometheus=True,
        enable_logging=True,
        log_interval_seconds=60
    )
    
    # Simulate window processing
    collector.record_window(WindowType.DAILY, record_count=1000, latency_ms=500)
    collector.record_window(WindowType.DAILY, record_count=1200, latency_ms=450)
    collector.record_window(WindowType.WEEKLY, record_count=7000, latency_ms=2000)
    collector.record_window(WindowType.MONTHLY, record_count=30000, latency_ms=8000)
    
    # Get metrics summary
    summary = collector.get_summary()
    logger.info("Metrics Summary:")
    for window_type, metrics in summary.items():
        if window_type == 'total':
            logger.info(f"Total: {metrics}")
        else:
            logger.info(f"{window_type}: {metrics}")
    
    # Export Prometheus metrics
    from flink_consumer.aggregations.metrics import PrometheusMetricsExporter
    exporter = PrometheusMetricsExporter(collector)
    prometheus_output = exporter.export_metrics()
    logger.info("Prometheus metrics:")
    logger.info(prometheus_output)


def example_schema_definitions():
    """
    Example: Aggregation table schemas.
    
    This example shows how to generate Iceberg DDL for aggregation tables.
    """
    logger.info("=== Example: Schema Definitions ===")
    
    from flink_consumer.aggregations import (
        DailyAggregateSchema,
        WeeklyAggregateSchema,
        MonthlyAggregateSchema,
    )
    
    # Generate DDL for daily aggregates
    daily_ddl = DailyAggregateSchema.get_iceberg_ddl()
    logger.info("Daily aggregates DDL:")
    logger.info(daily_ddl)
    
    # Generate DDL for weekly aggregates
    weekly_ddl = WeeklyAggregateSchema.get_iceberg_ddl()
    logger.info("\nWeekly aggregates DDL:")
    logger.info(weekly_ddl)
    
    # Generate DDL for monthly aggregates
    monthly_ddl = MonthlyAggregateSchema.get_iceberg_ddl()
    logger.info("\nMonthly aggregates DDL:")
    logger.info(monthly_ddl)


def example_metadata_enrichment():
    """
    Example: Window metadata enrichment.
    
    This example shows how metadata is calculated and added to aggregations.
    """
    logger.info("=== Example: Metadata Enrichment ===")
    
    from flink_consumer.aggregations import (
        MetadataCalculator,
        WindowType,
    )
    from flink_consumer.aggregations.metadata import AggregationMetadata
    
    timestamp_ms = int(datetime(2025, 11, 17, 15, 30, 0, tzinfo=timezone.utc).timestamp() * 1000)
    
    # Calculate aggregation date
    agg_date = MetadataCalculator.calculate_aggregation_date(timestamp_ms)
    logger.info(f"Aggregation date: {agg_date}")
    
    # Calculate week info
    week_info = MetadataCalculator.calculate_week_info(timestamp_ms)
    logger.info(f"Week info: {week_info}")
    
    # Calculate month info
    month_info = MetadataCalculator.calculate_month_info(timestamp_ms)
    logger.info(f"Month info: {month_info}")
    
    # Create complete metadata
    metadata = AggregationMetadata(
        user_id='user-123',
        data_type='heartRate',
        window_type=WindowType.DAILY,
        window_start=timestamp_ms,
        window_end=timestamp_ms + 86400000
    )
    
    logger.info(f"Complete metadata: {metadata.to_dict()}")


def main():
    """
    Run all examples.
    """
    logger.info("Starting aggregation pipeline examples")
    
    # Run examples
    example_schema_definitions()
    example_custom_window_configuration()
    example_metadata_enrichment()
    example_aggregation_validation()
    example_metrics_collection()
    example_basic_aggregation_pipeline()
    
    logger.info("All examples completed")


if __name__ == '__main__':
    main()
