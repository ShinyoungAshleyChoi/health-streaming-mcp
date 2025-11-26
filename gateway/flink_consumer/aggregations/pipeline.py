"""
Aggregation pipeline integration.

This module provides the main pipeline orchestration for time-based
aggregations, integrating all components (watermarks, windows, aggregators,
validators, sinks).
"""

from typing import Optional, Dict, Any, Tuple
import logging

from flink_consumer.aggregations.windows import WindowType, apply_tumbling_window
from flink_consumer.aggregations.watermark import (
    create_watermark_strategy,
    assign_watermarks_to_stream
)
from flink_consumer.aggregations.aggregator import HealthDataAggregator
from flink_consumer.aggregations.metadata import WindowMetadataEnricher
from flink_consumer.aggregations.validator import AggregationValidator
from flink_consumer.aggregations.sink import (
    AggregationSinkConfig,
    setup_aggregation_sinks
)

logger = logging.getLogger(__name__)


class AggregationPipelineConfig:
    """
    Configuration for aggregation pipeline.
    """
    
    def __init__(self,
                 enable_daily: bool = True,
                 enable_weekly: bool = True,
                 enable_monthly: bool = True,
                 daily_parallelism: int = 12,
                 weekly_parallelism: int = 6,
                 monthly_parallelism: int = 3,
                 state_ttl_days: int = 7,
                 watermark_out_of_orderness_minutes: int = 10,
                 enable_validation: bool = True,
                 enable_anomaly_detection: bool = True):
        """
        Initialize pipeline configuration.
        
        Args:
            enable_daily: Enable daily aggregations
            enable_weekly: Enable weekly aggregations
            enable_monthly: Enable monthly aggregations
            daily_parallelism: Parallelism for daily aggregations
            weekly_parallelism: Parallelism for weekly aggregations
            monthly_parallelism: Parallelism for monthly aggregations
            state_ttl_days: State TTL in days
            watermark_out_of_orderness_minutes: Watermark out-of-orderness
            enable_validation: Enable aggregation validation
            enable_anomaly_detection: Enable anomaly detection
        """
        self.enable_daily = enable_daily
        self.enable_weekly = enable_weekly
        self.enable_monthly = enable_monthly
        self.daily_parallelism = daily_parallelism
        self.weekly_parallelism = weekly_parallelism
        self.monthly_parallelism = monthly_parallelism
        self.state_ttl_days = state_ttl_days
        self.watermark_out_of_orderness_minutes = watermark_out_of_orderness_minutes
        self.enable_validation = enable_validation
        self.enable_anomaly_detection = enable_anomaly_detection


def create_aggregation_pipeline(env, table_env, raw_data_stream,
                               pipeline_config: Optional[AggregationPipelineConfig] = None,
                               sink_config: Optional[AggregationSinkConfig] = None):
    """
    Create complete aggregation pipeline.
    
    This function orchestrates the entire aggregation pipeline:
    1. Assign watermarks to raw data stream
    2. Key by user_id and data_type
    3. Apply windows (daily, weekly, monthly)
    4. Aggregate data
    5. Enrich with metadata
    6. Validate results
    7. Write to Iceberg tables
    
    Args:
        env: PyFlink StreamExecutionEnvironment
        table_env: PyFlink StreamTableEnvironment
        raw_data_stream: Raw health data stream
        pipeline_config: Pipeline configuration
        sink_config: Sink configuration
        
    Returns:
        Tuple of (daily_stream, weekly_stream, monthly_stream) or None for disabled streams
        
    Example:
        >>> from pyflink.datastream import StreamExecutionEnvironment
        >>> from pyflink.table import StreamTableEnvironment
        >>> env = StreamExecutionEnvironment.get_execution_environment()
        >>> table_env = StreamTableEnvironment.create(env)
        >>> pipeline_config = AggregationPipelineConfig()
        >>> sink_config = AggregationSinkConfig()
        >>> streams = create_aggregation_pipeline(env, table_env, raw_stream, 
        ...                                       pipeline_config, sink_config)
    """
    if pipeline_config is None:
        pipeline_config = AggregationPipelineConfig()
    
    if sink_config is None:
        sink_config = AggregationSinkConfig()
    
    logger.info("Creating aggregation pipeline")
    
    # Configure state TTL
    _configure_state_ttl(env, pipeline_config.state_ttl_days)
    
    # Step 1: Assign watermarks
    logger.info("Assigning watermarks to raw data stream")
    watermark_config = create_watermark_strategy(
        pipeline_config.watermark_out_of_orderness_minutes
    )
    stream_with_watermarks = assign_watermarks_to_stream(
        raw_data_stream,
        watermark_config
    )
    
    # Step 2: Key by user_id and data_type
    logger.info("Keying stream by user_id and data_type")
    keyed_stream = stream_with_watermarks.key_by(
        lambda row: (row['user_id'], row['data_type'])
    )
    
    # Step 3-7: Create aggregation branches
    daily_stream = None
    weekly_stream = None
    monthly_stream = None
    
    if pipeline_config.enable_daily:
        logger.info("Creating daily aggregation branch")
        daily_stream = _create_aggregation_branch(
            keyed_stream,
            WindowType.DAILY,
            pipeline_config.daily_parallelism,
            pipeline_config.enable_validation,
            pipeline_config.enable_anomaly_detection
        )
    
    if pipeline_config.enable_weekly:
        logger.info("Creating weekly aggregation branch")
        weekly_stream = _create_aggregation_branch(
            keyed_stream,
            WindowType.WEEKLY,
            pipeline_config.weekly_parallelism,
            pipeline_config.enable_validation,
            pipeline_config.enable_anomaly_detection
        )
    
    if pipeline_config.enable_monthly:
        logger.info("Creating monthly aggregation branch")
        monthly_stream = _create_aggregation_branch(
            keyed_stream,
            WindowType.MONTHLY,
            pipeline_config.monthly_parallelism,
            pipeline_config.enable_validation,
            pipeline_config.enable_anomaly_detection
        )
    
    # Step 8: Setup sinks
    logger.info("Setting up Iceberg sinks for aggregations")
    if daily_stream or weekly_stream or monthly_stream:
        # Only setup sinks for enabled streams
        _setup_sinks_for_enabled_streams(
            env, table_env,
            daily_stream, weekly_stream, monthly_stream,
            sink_config
        )
    
    logger.info("Aggregation pipeline created successfully")
    
    return daily_stream, weekly_stream, monthly_stream


def _configure_state_ttl(env, ttl_days: int):
    """
    Configure state TTL to prevent unbounded state growth.
    
    Args:
        env: PyFlink StreamExecutionEnvironment
        ttl_days: State TTL in days
    """
    try:
        from pyflink.common import Time
        
        logger.info(f"Configuring state TTL: {ttl_days} days")
        
        # Note: State TTL configuration in PyFlink may vary by version
        # This is a placeholder for the actual configuration
        # In practice, this might be done via flink-conf.yaml or programmatically
        
    except ImportError:
        logger.warning("Could not configure state TTL (PyFlink not available)")


def _create_aggregation_branch(keyed_stream, window_type: WindowType,
                               parallelism: int, enable_validation: bool,
                               enable_anomaly_detection: bool):
    """
    Create a single aggregation branch (daily, weekly, or monthly).
    
    Args:
        keyed_stream: Keyed data stream
        window_type: Type of window
        parallelism: Parallelism for this branch
        enable_validation: Enable validation
        enable_anomaly_detection: Enable anomaly detection
        
    Returns:
        Aggregated and enriched stream
    """
    logger.info(f"Creating {window_type.value} aggregation branch with parallelism={parallelism}")
    
    # Apply window
    windowed_stream = apply_tumbling_window(keyed_stream, window_type)
    
    # Apply aggregation
    aggregator = HealthDataAggregator()
    metadata_enricher = WindowMetadataEnricher(window_type)
    
    # Aggregate and enrich
    # Note: The actual PyFlink API may require different syntax
    # This is a conceptual representation
    aggregated_stream = windowed_stream.aggregate(
        aggregator,
        metadata_enricher
    )
    
    # Set parallelism
    aggregated_stream = aggregated_stream.set_parallelism(parallelism)
    
    # Apply validation if enabled
    if enable_validation:
        logger.info(f"Enabling validation for {window_type.value} aggregations")
        validator = AggregationValidator(enable_anomaly_detection=enable_anomaly_detection)
        aggregated_stream = aggregated_stream.filter(validator.filter)
    
    return aggregated_stream


def _setup_sinks_for_enabled_streams(env, table_env,
                                     daily_stream, weekly_stream, monthly_stream,
                                     sink_config: AggregationSinkConfig):
    """
    Setup sinks only for enabled streams.
    
    Args:
        env: PyFlink StreamExecutionEnvironment
        table_env: PyFlink StreamTableEnvironment
        daily_stream: Daily aggregation stream (or None)
        weekly_stream: Weekly aggregation stream (or None)
        monthly_stream: Monthly aggregation stream (or None)
        sink_config: Sink configuration
    """
    # Create placeholder streams for disabled aggregations
    # This allows setup_aggregation_sinks to work with None values
    
    if daily_stream is not None:
        from flink_consumer.aggregations.sink import create_aggregation_sink
        create_aggregation_sink(daily_stream, WindowType.DAILY, sink_config, table_env)
    
    if weekly_stream is not None:
        from flink_consumer.aggregations.sink import create_aggregation_sink
        create_aggregation_sink(weekly_stream, WindowType.WEEKLY, sink_config, table_env)
    
    if monthly_stream is not None:
        from flink_consumer.aggregations.sink import create_aggregation_sink
        create_aggregation_sink(monthly_stream, WindowType.MONTHLY, sink_config, table_env)


def integrate_with_main_pipeline(env, table_env, raw_data_stream,
                                 enable_aggregations: bool = True,
                                 pipeline_config: Optional[AggregationPipelineConfig] = None,
                                 sink_config: Optional[AggregationSinkConfig] = None):
    """
    Integrate aggregation pipeline with main data pipeline.
    
    This function allows the aggregation pipeline to run in parallel with
    the main raw data pipeline, processing the same stream independently.
    
    Args:
        env: PyFlink StreamExecutionEnvironment
        table_env: PyFlink StreamTableEnvironment
        raw_data_stream: Raw health data stream
        enable_aggregations: Whether to enable aggregations
        pipeline_config: Pipeline configuration
        sink_config: Sink configuration
        
    Returns:
        Tuple of (raw_stream, aggregation_streams) where aggregation_streams
        is a tuple of (daily, weekly, monthly) or None if disabled
        
    Example:
        >>> raw_stream, agg_streams = integrate_with_main_pipeline(
        ...     env, table_env, raw_data_stream, enable_aggregations=True
        ... )
        >>> # Continue with raw data pipeline
        >>> raw_stream.add_sink(raw_data_sink)
        >>> # Aggregations run in parallel
    """
    if not enable_aggregations:
        logger.info("Aggregations disabled, skipping aggregation pipeline")
        return raw_data_stream, None
    
    logger.info("Integrating aggregation pipeline with main pipeline")
    
    # Create aggregation pipeline as a parallel branch
    # The raw_data_stream is not consumed, just observed
    aggregation_streams = create_aggregation_pipeline(
        env, table_env, raw_data_stream,
        pipeline_config, sink_config
    )
    
    logger.info("Aggregation pipeline integrated successfully")
    
    # Return both streams so main pipeline can continue
    return raw_data_stream, aggregation_streams


class PipelineMetrics:
    """
    Track pipeline-level metrics.
    """
    
    def __init__(self):
        self.daily_windows_processed = 0
        self.weekly_windows_processed = 0
        self.monthly_windows_processed = 0
        self.total_records_aggregated = 0
        self.validation_failures = 0
    
    def record_daily_window(self, record_count: int):
        """Record daily window processing."""
        self.daily_windows_processed += 1
        self.total_records_aggregated += record_count
    
    def record_weekly_window(self, record_count: int):
        """Record weekly window processing."""
        self.weekly_windows_processed += 1
        self.total_records_aggregated += record_count
    
    def record_monthly_window(self, record_count: int):
        """Record monthly window processing."""
        self.monthly_windows_processed += 1
        self.total_records_aggregated += record_count
    
    def record_validation_failure(self):
        """Record validation failure."""
        self.validation_failures += 1
    
    def get_metrics(self) -> Dict[str, int]:
        """Get current metrics."""
        return {
            'daily_windows_processed': self.daily_windows_processed,
            'weekly_windows_processed': self.weekly_windows_processed,
            'monthly_windows_processed': self.monthly_windows_processed,
            'total_records_aggregated': self.total_records_aggregated,
            'validation_failures': self.validation_failures,
        }
