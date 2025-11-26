"""
Aggregation pipeline components for time-based health data aggregations.

This package provides a complete aggregation pipeline for health data:
- Time-based windows (daily, weekly, monthly)
- Statistical aggregations (min, max, avg, sum, count, stddev)
- Data validation and quality checks
- Iceberg sink with upsert support
- Monitoring metrics
"""

from flink_consumer.aggregations.schemas import (
    DailyAggregateSchema,
    WeeklyAggregateSchema,
    MonthlyAggregateSchema,
)
from flink_consumer.aggregations.windows import (
    WindowType,
    WindowConfig,
    create_tumbling_window,
    apply_tumbling_window,
)
from flink_consumer.aggregations.watermark import (
    TimestampExtractor,
    create_watermark_strategy,
    assign_watermarks_to_stream,
)
from flink_consumer.aggregations.aggregator import (
    HealthDataAggregator,
    HealthDataAccumulator,
)
from flink_consumer.aggregations.metadata import (
    WindowMetadataEnricher,
    MetadataCalculator,
)
from flink_consumer.aggregations.validator import (
    AggregationValidator,
    DataQualityChecker,
)
from flink_consumer.aggregations.sink import (
    AggregationSinkConfig,
    create_aggregation_sink,
    setup_aggregation_sinks,
)
from flink_consumer.aggregations.pipeline import (
    AggregationPipelineConfig,
    create_aggregation_pipeline,
    integrate_with_main_pipeline,
)
from flink_consumer.aggregations.metrics import (
    AggregationMetrics,
    MetricsCollector,
    create_metrics_reporter,
    setup_metrics_collection,
)

__all__ = [
    # Schemas
    'DailyAggregateSchema',
    'WeeklyAggregateSchema',
    'MonthlyAggregateSchema',
    # Windows
    'WindowType',
    'WindowConfig',
    'create_tumbling_window',
    'apply_tumbling_window',
    # Watermarks
    'TimestampExtractor',
    'create_watermark_strategy',
    'assign_watermarks_to_stream',
    # Aggregators
    'HealthDataAggregator',
    'HealthDataAccumulator',
    # Metadata
    'WindowMetadataEnricher',
    'MetadataCalculator',
    # Validators
    'AggregationValidator',
    'DataQualityChecker',
    # Sinks
    'AggregationSinkConfig',
    'create_aggregation_sink',
    'setup_aggregation_sinks',
    # Pipeline
    'AggregationPipelineConfig',
    'create_aggregation_pipeline',
    'integrate_with_main_pipeline',
    # Metrics
    'AggregationMetrics',
    'MetricsCollector',
    'create_metrics_reporter',
    'setup_metrics_collection',
]
