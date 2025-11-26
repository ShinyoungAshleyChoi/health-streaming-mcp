"""
Main entry point for Flink Iceberg Consumer application.

This module orchestrates the complete data pipeline:
1. Load and validate configuration
2. Initialize Flink execution environment
3. Configure checkpoint and recovery
4. Set up Kafka source with Avro deserialization
5. Apply data transformation and validation
6. Write to Iceberg tables (raw data and errors)
7. Execute the Flink job

Requirements: 1.1, 8.1
"""

import logging
import sys
from typing import Optional

from pyflink.datastream import StreamExecutionEnvironment

# Configuration
from flink_consumer.config.config import load_config
from flink_consumer.config.logging import setup_logging
from flink_consumer.config.checkpoint import setup_checkpoint_and_state
from flink_consumer.config.recovery import setup_recovery_strategy

# Kafka source
from flink_consumer.services.kafka_source import add_kafka_source_to_env

# Data transformation and validation
from flink_consumer.converters.health_data_transformer import create_transformer
from flink_consumer.validators.health_data_validator import create_validator

# Error handling
from flink_consumer.services.error_handler import (
    create_validation_process_function,
    create_error_enricher,
    get_error_output_tag
)

# Iceberg sink
from flink_consumer.iceberg.sink import IcebergSink, IcebergErrorSink
from flink_consumer.iceberg.table_manager import IcebergTableManager

# Aggregations
from flink_consumer.aggregations.calendar_aggregator import (
    CalendarDailyAggregator,
    CalendarMonthlyAggregator
)
from flink_consumer.aggregations.watermark import create_watermark_strategy

# Metrics
from flink_consumer.services.metrics import create_metrics_reporter

logger = logging.getLogger(__name__)


class FlinkIcebergConsumer:
    """
    Main application class for Flink Iceberg Consumer.
    
    Orchestrates the complete data pipeline from Kafka to Iceberg,
    including transformation, validation, error handling, and monitoring.
    """

    def __init__(self):
        """Initialize the Flink Iceberg Consumer application"""
        self.settings = None
        self.env: Optional[StreamExecutionEnvironment] = None
        self.iceberg_sink: Optional[IcebergSink] = None
        self.table_manager: Optional[IcebergTableManager] = None

    def initialize(self) -> bool:
        """
        Initialize the application by loading configuration and setting up logging.
        
        Returns:
            True if initialization successful, False otherwise
        """
        try:
            logger.info("=" * 80)
            logger.info("FLINK ICEBERG CONSUMER - INITIALIZATION")
            logger.info("=" * 80)
            
            # Load and validate configuration
            logger.info("Loading configuration...")
            self.settings = load_config()
            
            # Setup logging with configured level and format
            setup_logging(
                log_level=self.settings.monitoring.log_level,
                # log_format=self.settings.monitoring.log_format
            )
            
            logger.info(
                f"Application: {self.settings.app.app_name} "
                f"v{self.settings.app.app_version}"
            )
            logger.info(f"Environment: {self.settings.app.environment}")
            logger.info("Configuration loaded and validated successfully")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize application: {e}", exc_info=True)
            return False

    def setup_execution_environment(self) -> bool:
        """
        Set up Flink execution environment with checkpoint and recovery configuration.
        
        Returns:
            True if setup successful, False otherwise
        """
        try:
            logger.info("=" * 80)
            logger.info("SETTING UP FLINK EXECUTION ENVIRONMENT")
            logger.info("=" * 80)
            
            # Create execution environment
            self.env = StreamExecutionEnvironment.get_execution_environment()
            logger.info("StreamExecutionEnvironment created")
            
            # Set parallelism
            self.env.set_parallelism(self.settings.flink.parallelism)
            logger.info(f"Parallelism set to: {self.settings.flink.parallelism}")
            
            # Configure checkpoint and state management
            logger.info("Configuring checkpoint and state management...")
            setup_checkpoint_and_state(self.env, self.settings.flink)
            
            # Configure recovery strategy
            logger.info("Configuring recovery strategy...")
            setup_recovery_strategy(self.env, self.settings.flink)
            
            logger.info("Flink execution environment setup completed")
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup execution environment: {e}", exc_info=True)
            return False

    def setup_iceberg_infrastructure(self) -> bool:
        """
        Set up Iceberg catalog, database, and tables.
        
        Returns:
            True if setup successful, False otherwise
        """
        try:
            logger.info("=" * 80)
            logger.info("SETTING UP ICEBERG INFRASTRUCTURE")
            logger.info("=" * 80)
            
            # Create Iceberg sink
            self.iceberg_sink = IcebergSink(self.settings, self.env)
            
            # Initialize catalog and database
            logger.info("Initializing Iceberg catalog and database...")
            if not self.iceberg_sink.initialize():
                logger.error("Failed to initialize Iceberg sink")
                return False
            
            # Create table manager
            self.table_manager = IcebergTableManager(self.settings)
            
            # Ensure raw data table exists
            logger.info(f"Ensuring table exists: {self.settings.iceberg.table_raw}")
            if not self.table_manager.ensure_raw_table_exists():
                logger.error("Failed to ensure raw data table exists")
                return False
            
            # Ensure error table exists
            logger.info(f"Ensuring table exists: {self.settings.iceberg.table_errors}")
            if not self.table_manager.ensure_error_table_exists():
                logger.error("Failed to ensure error table exists")
                return False
            
            # Ensure aggregation tables exist
            logger.info(f"Ensuring table exists: {self.settings.iceberg.table_daily_agg}")
            if not self.table_manager.ensure_daily_agg_table_exists():
                logger.error("Failed to ensure daily aggregation table exists")
                return False
            
            logger.info(f"Ensuring table exists: {self.settings.iceberg.table_weekly_agg}")
            if not self.table_manager.ensure_weekly_agg_table_exists():
                logger.error("Failed to ensure weekly aggregation table exists")
                return False
            
            logger.info(f"Ensuring table exists: {self.settings.iceberg.table_monthly_agg}")
            if not self.table_manager.ensure_monthly_agg_table_exists():
                logger.error("Failed to ensure monthly aggregation table exists")
                return False
            
            logger.info("Iceberg infrastructure setup completed")
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup Iceberg infrastructure: {e}", exc_info=True)
            return False

    def build_pipeline(self) -> bool:
        """
        Build the complete data processing pipeline.
        
        Pipeline flow:
        1. Kafka Source (Avro deserialization)
        2. Data Transformation (flatten nested structure)
        3. Data Validation (quality checks)
        4. Error Handling (side output for invalid records)
        5. Metrics Reporting
        6. Iceberg Sink (raw data and errors)
        
        Returns:
            True if pipeline built successfully, False otherwise
        """
        try:
            logger.info("=" * 80)
            logger.info("BUILDING DATA PROCESSING PIPELINE")
            logger.info("=" * 80)
            
            # Step 1: Create Kafka source
            logger.info("Step 1: Creating Kafka source...")
            kafka_stream = add_kafka_source_to_env(
                env=self.env,
                kafka_settings=self.settings.kafka,
                schema_registry_settings=self.settings.schema_registry,
                source_name="Kafka Health Data Source"
            )
            logger.info("Kafka source created")
            
            # Step 2: Apply data transformation
            logger.info("Step 2: Applying data transformation...")
            transformer = create_transformer()
            transformed_stream = kafka_stream.flat_map(
                transformer,
                output_type=None
            ).name("Health Data Transformer")
            logger.info("Data transformation configured")
            
            # Step 3: Apply validation with error handling
            logger.info("Step 3: Applying validation with error handling...")
            validation_function = create_validation_process_function(
                strict_validation=False
            )
            validated_stream = transformed_stream.process(
                validation_function,
                output_type=None
            ).name("Health Data Validator")
            
            # Get error stream from side output
            error_output_tag = get_error_output_tag()
            error_stream = validated_stream.get_side_output(error_output_tag)
            logger.info("Validation and error handling configured")
            
            # Step 4: Apply metrics reporting to valid stream
            logger.info("Step 4: Applying metrics reporting...")
            metrics_reporter = create_metrics_reporter()
            monitored_stream = validated_stream.map(
                metrics_reporter,
                output_type=None
            ).name("Metrics Reporter")
            logger.info("Metrics reporting configured")
            
            # Step 5: Enrich error records
            logger.info("Step 5: Enriching error records...")
            error_enricher = create_error_enricher()
            enriched_errors = error_stream.process(
                error_enricher,
                output_type=None
            ).name("Error Enricher")
            logger.info("Error enrichment configured")
            
            # Step 6: Write valid data to Iceberg raw table
            logger.info("Step 6: Writing valid data to Iceberg...")
            success = self.iceberg_sink.write_to_iceberg(
                data_stream=monitored_stream,
                table_name=self.settings.iceberg.table_raw,
                overwrite=False
            )
            if not success:
                logger.error("Failed to configure Iceberg sink for raw data")
                return False
            logger.info(f"Iceberg sink configured for table: {self.settings.iceberg.table_raw}")
            
            # Step 7: Write error data to Iceberg error table
            logger.info("Step 7: Writing error data to Iceberg...")
            error_sink = IcebergErrorSink(self.settings, self.iceberg_sink)
            success = error_sink.write_errors_to_iceberg(enriched_errors)
            if not success:
                logger.error("Failed to configure Iceberg sink for errors")
                return False
            logger.info(f"Iceberg error sink configured for table: {self.settings.iceberg.table_errors}")
            
            # Step 8: Set up aggregation pipeline
            logger.info("=" * 80)
            logger.info("SETTING UP AGGREGATION PIPELINE")
            logger.info("=" * 80)
            
            # Assign watermarks for event time processing
            logger.info("Step 8a: Assigning watermarks for event time...")
            watermark_strategy = create_watermark_strategy(
                max_out_of_orderness_minutes=10,
                timestamp_field='start_date'
            )
            stream_with_watermarks = monitored_stream.assign_timestamps_and_watermarks(
                watermark_strategy
            ).name("Watermark Assigner")
            logger.info("Watermarks assigned (10 minutes out-of-orderness)")
            
            # Key by (user_id, data_type) for aggregations
            logger.info("Step 8b: Keying stream by (user_id, data_type)...")
            keyed_stream = stream_with_watermarks.key_by(
                lambda row: (row['user_id'], row['data_type'])
            ).name("Keyed Stream")
            logger.info("Stream keyed by (user_id, data_type)")
            
            # Daily aggregations
            logger.info("Step 8c: Setting up daily aggregations...")
            daily_aggregator = CalendarDailyAggregator(
                default_timezone="UTC",
                emit_on_watermark=True
            )
            daily_agg_stream = keyed_stream.process(
                daily_aggregator,
                output_type=None
            ).name("Daily Aggregator")
            
            # Set parallelism for daily aggregations
            daily_agg_stream.set_parallelism(12)
            logger.info("Daily aggregations configured (parallelism: 12)")
            
            # Monthly aggregations
            logger.info("Step 8d: Setting up monthly aggregations...")
            monthly_aggregator = CalendarMonthlyAggregator(
                default_timezone="UTC"
            )
            monthly_agg_stream = keyed_stream.process(
                monthly_aggregator,
                output_type=None
            ).name("Monthly Aggregator")
            
            # Set parallelism for monthly aggregations
            monthly_agg_stream.set_parallelism(3)
            logger.info("Monthly aggregations configured (parallelism: 3)")
            
            # Write daily aggregations to Iceberg
            logger.info("Step 8e: Writing daily aggregations to Iceberg...")
            success = self.iceberg_sink.write_to_iceberg(
                data_stream=daily_agg_stream,
                table_name=self.settings.iceberg.table_daily_agg,
                overwrite=False
            )
            if not success:
                logger.error("Failed to configure Iceberg sink for daily aggregations")
                return False
            logger.info(f"Daily aggregation sink configured: {self.settings.iceberg.table_daily_agg}")
            
            # Write monthly aggregations to Iceberg
            logger.info("Step 8f: Writing monthly aggregations to Iceberg...")
            success = self.iceberg_sink.write_to_iceberg(
                data_stream=monthly_agg_stream,
                table_name=self.settings.iceberg.table_monthly_agg,
                overwrite=False
            )
            if not success:
                logger.error("Failed to configure Iceberg sink for monthly aggregations")
                return False
            logger.info(f"Monthly aggregation sink configured: {self.settings.iceberg.table_monthly_agg}")
            
            logger.info("=" * 80)
            logger.info("PIPELINE CONSTRUCTION COMPLETED")
            logger.info("=" * 80)
            logger.info("Pipeline flow:")
            logger.info("  1. Kafka Source (Avro deserialization)")
            logger.info("  2. Health Data Transformer (flatten nested structure)")
            logger.info("  3. Health Data Validator (quality checks)")
            logger.info("  4. Error Handler (side output for invalid records)")
            logger.info("  5. Metrics Reporter (monitoring)")
            logger.info(f"  6. Iceberg Sink → {self.settings.iceberg.table_raw}")
            logger.info(f"  7. Error Sink → {self.settings.iceberg.table_errors}")
            logger.info("  8. Aggregation Pipeline:")
            logger.info("     a. Watermark Assignment (10 min out-of-orderness)")
            logger.info("     b. Key by (user_id, data_type)")
            logger.info(f"     c. Daily Aggregations → {self.settings.iceberg.table_daily_agg}")
            logger.info(f"     d. Monthly Aggregations → {self.settings.iceberg.table_monthly_agg}")
            logger.info("=" * 80)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to build pipeline: {e}", exc_info=True)
            return False

    def execute(self) -> bool:
        """
        Execute the Flink job.
        
        This is a blocking call that will run until the job is cancelled
        or fails.
        
        Returns:
            True if job completed successfully, False otherwise
        """
        try:
            logger.info("=" * 80)
            logger.info("STARTING FLINK JOB EXECUTION")
            logger.info("=" * 80)
            
            job_name = f"{self.settings.app.app_name}-{self.settings.app.app_version}"
            
            logger.info(f"Job name: {job_name}")
            logger.info(f"Kafka topic: {self.settings.kafka.topic}")
            logger.info(f"Consumer group: {self.settings.kafka.group_id}")
            logger.info(f"Iceberg catalog: {self.settings.iceberg.catalog_name}")
            logger.info(f"Iceberg database: {self.settings.iceberg.database}")
            logger.info(f"Parallelism: {self.settings.flink.parallelism}")
            logger.info(f"Checkpoint interval: {self.settings.flink.checkpoint_interval_ms}ms")
            logger.info(f"Checkpoint mode: {self.settings.flink.checkpoint_mode}")
            
            logger.info("=" * 80)
            logger.info("Job is starting... (this is a blocking operation)")
            logger.info("Press Ctrl+C to stop the job")
            logger.info("=" * 80)
            
            # Execute the job (blocking call)
            result = self.env.execute(job_name)
            
            logger.info("=" * 80)
            logger.info("JOB COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            logger.info(f"Job result: {result}")
            
            return True
            
        except KeyboardInterrupt:
            logger.info("=" * 80)
            logger.info("JOB INTERRUPTED BY USER")
            logger.info("=" * 80)
            return True
            
        except Exception as e:
            logger.error("=" * 80)
            logger.error("JOB EXECUTION FAILED")
            logger.error("=" * 80)
            logger.error(f"Error: {e}", exc_info=True)
            return False

    def run(self) -> int:
        """
        Run the complete application lifecycle.
        
        Returns:
            Exit code (0 for success, 1 for failure)
        """
        try:
            # Initialize application
            if not self.initialize():
                logger.error("Application initialization failed")
                return 1
            
            # Setup execution environment
            if not self.setup_execution_environment():
                logger.error("Execution environment setup failed")
                return 1
            
            # Setup Iceberg infrastructure
            if not self.setup_iceberg_infrastructure():
                logger.error("Iceberg infrastructure setup failed")
                return 1
            
            # Build pipeline
            if not self.build_pipeline():
                logger.error("Pipeline construction failed")
                return 1
            
            # Execute job
            if not self.execute():
                logger.error("Job execution failed")
                return 1
            
            return 0
            
        except Exception as e:
            logger.error(f"Unexpected error in application: {e}", exc_info=True)
            return 1


def main() -> int:
    """
    Main entry point for the application.
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Create and run application
    app = FlinkIcebergConsumer()
    exit_code = app.run()
    
    # Exit with appropriate code
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
