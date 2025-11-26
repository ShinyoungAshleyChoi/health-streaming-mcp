"""Iceberg table schema definitions"""

from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    DoubleType,
    BooleanType,
    TimestampType,
    MapType,
    LongType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform


def get_health_data_raw_schema() -> Schema:
    """
    Get schema for health_data_raw table
    
    Returns:
        Iceberg Schema for raw health data
    """
    return Schema(
        NestedField(
            field_id=1,
            name="device_id",
            field_type=StringType(),
            required=True,
            doc="Device identifier"
        ),
        NestedField(
            field_id=2,
            name="user_id",
            field_type=StringType(),
            required=True,
            doc="User identifier"
        ),
        NestedField(
            field_id=3,
            name="sample_id",
            field_type=StringType(),
            required=True,
            doc="Sample identifier"
        ),
        NestedField(
            field_id=4,
            name="data_type",
            field_type=StringType(),
            required=True,
            doc="Health data type (heartRate, steps, etc.)"
        ),
        NestedField(
            field_id=5,
            name="value",
            field_type=DoubleType(),
            required=True,
            doc="Measurement value"
        ),
        NestedField(
            field_id=6,
            name="unit",
            field_type=StringType(),
            required=True,
            doc="Unit of measurement"
        ),
        NestedField(
            field_id=7,
            name="start_date",
            field_type=TimestampType(),
            required=True,
            doc="Start timestamp of measurement"
        ),
        NestedField(
            field_id=8,
            name="end_date",
            field_type=TimestampType(),
            required=True,
            doc="End timestamp of measurement"
        ),
        NestedField(
            field_id=9,
            name="source_bundle",
            field_type=StringType(),
            required=False,
            doc="Source bundle identifier"
        ),
        NestedField(
            field_id=10,
            name="metadata",
            field_type=MapType(
                key_id=11,
                key_type=StringType(),
                value_id=12,
                value_type=StringType(),
                value_required=False
            ),
            required=False,
            doc="Additional metadata as key-value pairs"
        ),
        NestedField(
            field_id=13,
            name="is_synced",
            field_type=BooleanType(),
            required=True,
            doc="Sync status flag"
        ),
        NestedField(
            field_id=14,
            name="created_at",
            field_type=TimestampType(),
            required=True,
            doc="Record creation timestamp"
        ),
        NestedField(
            field_id=15,
            name="payload_timestamp",
            field_type=TimestampType(),
            required=True,
            doc="Original payload timestamp"
        ),
        NestedField(
            field_id=16,
            name="app_version",
            field_type=StringType(),
            required=True,
            doc="Application version"
        ),
        NestedField(
            field_id=17,
            name="processing_time",
            field_type=TimestampType(),
            required=True,
            doc="Flink processing timestamp"
        ),
        NestedField(
            field_id=18,
            name="timezone",
            field_type=StringType(),
            required=True,
            doc="IANA timezone identifier (e.g., Asia/Seoul)"
        ),
        NestedField(
            field_id=19,
            name="timezone_offset",
            field_type=LongType(),
            required=False,
            doc="Timezone offset in seconds from UTC"
        ),
    )


def get_health_data_raw_partition_spec() -> PartitionSpec:
    """
    Get partition specification for health_data_raw table
    
    Partitions by:
    - days(start_date): Daily partitions based on measurement start date
    - data_type: Sub-partitions by health data type
    
    Returns:
        Iceberg PartitionSpec
    """
    return PartitionSpec(
        PartitionField(
            source_id=7,  # start_date field
            field_id=1000,
            transform=DayTransform(),
            name="start_date_day"
        ),
        PartitionField(
            source_id=4,  # data_type field
            field_id=1001,
            transform=IdentityTransform(),
            name="data_type"
        ),
    )


def get_health_data_raw_properties() -> dict:
    """
    Get table properties for health_data_raw table
    
    Returns:
        Dictionary of table properties
    """
    return {
        "write.format.default": "parquet",
        "write.parquet.compression-codec": "snappy",
        "write.metadata.compression-codec": "gzip",
        "commit.retry.num-retries": "3",
        "commit.retry.min-wait-ms": "100",
        "write.parquet.page-size-bytes": "1048576",  # 1MB
        "write.parquet.row-group-size-bytes": "134217728",  # 128MB
        "write.target-file-size-bytes": "268435456",  # 256MB
    }


def get_health_data_errors_schema() -> Schema:
    """
    Get schema for health_data_errors table (DLQ)
    
    Returns:
        Iceberg Schema for error records
    """
    return Schema(
        NestedField(
            field_id=1,
            name="error_id",
            field_type=StringType(),
            required=True,
            doc="Error record identifier"
        ),
        NestedField(
            field_id=2,
            name="error_type",
            field_type=StringType(),
            required=True,
            doc="Type of error (validation, deserialization, etc.)"
        ),
        NestedField(
            field_id=3,
            name="error_message",
            field_type=StringType(),
            required=True,
            doc="Error message description"
        ),
        NestedField(
            field_id=4,
            name="error_timestamp",
            field_type=TimestampType(),
            required=True,
            doc="When the error occurred"
        ),
        NestedField(
            field_id=5,
            name="raw_payload",
            field_type=StringType(),
            required=False,
            doc="Original raw payload that caused the error"
        ),
        NestedField(
            field_id=6,
            name="user_id",
            field_type=StringType(),
            required=False,
            doc="User identifier if available"
        ),
        NestedField(
            field_id=7,
            name="device_id",
            field_type=StringType(),
            required=False,
            doc="Device identifier if available"
        ),
        NestedField(
            field_id=8,
            name="sample_id",
            field_type=StringType(),
            required=False,
            doc="Sample identifier if available"
        ),
        NestedField(
            field_id=9,
            name="data_type",
            field_type=StringType(),
            required=False,
            doc="Health data type if available"
        ),
        NestedField(
            field_id=10,
            name="kafka_topic",
            field_type=StringType(),
            required=True,
            doc="Source Kafka topic"
        ),
        NestedField(
            field_id=11,
            name="kafka_partition",
            field_type=LongType(),
            required=True,
            doc="Source Kafka partition"
        ),
        NestedField(
            field_id=12,
            name="kafka_offset",
            field_type=LongType(),
            required=True,
            doc="Source Kafka offset"
        ),
    )


def get_health_data_errors_partition_spec() -> PartitionSpec:
    """
    Get partition specification for health_data_errors table
    
    Partitions by:
    - days(error_timestamp): Daily partitions based on error timestamp
    - error_type: Sub-partitions by error type
    
    Returns:
        Iceberg PartitionSpec
    """
    return PartitionSpec(
        PartitionField(
            source_id=4,  # error_timestamp field
            field_id=1000,
            transform=DayTransform(),
            name="error_date"
        ),
        PartitionField(
            source_id=2,  # error_type field
            field_id=1001,
            transform=IdentityTransform(),
            name="error_type"
        ),
    )


def get_health_data_errors_properties() -> dict:
    """
    Get table properties for health_data_errors table
    
    Returns:
        Dictionary of table properties
    """
    return {
        "write.format.default": "parquet",
        "write.parquet.compression-codec": "snappy",
        "write.metadata.compression-codec": "gzip",
        "commit.retry.num-retries": "3",
        "commit.retry.min-wait-ms": "100",
    }


# ============================================================================
# Aggregation Table Schemas
# ============================================================================


def get_health_data_daily_agg_schema() -> Schema:
    """
    Get schema for health_data_daily_agg table
    
    Daily aggregated statistics for health data.
    
    Returns:
        Iceberg Schema for daily aggregates
    """
    return Schema(
        NestedField(
            field_id=1,
            name="user_id",
            field_type=StringType(),
            required=True,
            doc="User identifier"
        ),
        NestedField(
            field_id=2,
            name="data_type",
            field_type=StringType(),
            required=True,
            doc="Health data type (heartRate, steps, etc.)"
        ),
        NestedField(
            field_id=3,
            name="aggregation_date",
            field_type=StringType(),  # DATE as string in format YYYY-MM-DD
            required=True,
            doc="Aggregation date (YYYY-MM-DD)"
        ),
        NestedField(
            field_id=4,
            name="window_start",
            field_type=TimestampType(),
            required=True,
            doc="Window start timestamp"
        ),
        NestedField(
            field_id=5,
            name="window_end",
            field_type=TimestampType(),
            required=True,
            doc="Window end timestamp"
        ),
        NestedField(
            field_id=6,
            name="min_value",
            field_type=DoubleType(),
            required=True,
            doc="Minimum value in window"
        ),
        NestedField(
            field_id=7,
            name="max_value",
            field_type=DoubleType(),
            required=True,
            doc="Maximum value in window"
        ),
        NestedField(
            field_id=8,
            name="avg_value",
            field_type=DoubleType(),
            required=True,
            doc="Average value in window"
        ),
        NestedField(
            field_id=9,
            name="sum_value",
            field_type=DoubleType(),
            required=True,
            doc="Sum of values in window"
        ),
        NestedField(
            field_id=10,
            name="count",
            field_type=LongType(),
            required=True,
            doc="Count of values in window"
        ),
        NestedField(
            field_id=11,
            name="stddev_value",
            field_type=DoubleType(),
            required=True,
            doc="Standard deviation of values"
        ),
        NestedField(
            field_id=12,
            name="first_value",
            field_type=DoubleType(),
            required=False,
            doc="First value in window"
        ),
        NestedField(
            field_id=13,
            name="last_value",
            field_type=DoubleType(),
            required=False,
            doc="Last value in window"
        ),
        NestedField(
            field_id=14,
            name="record_count",
            field_type=LongType(),
            required=True,
            doc="Number of records aggregated"
        ),
        NestedField(
            field_id=15,
            name="updated_at",
            field_type=TimestampType(),
            required=True,
            doc="Last update timestamp"
        ),
        NestedField(
            field_id=16,
            name="timezone",
            field_type=StringType(),
            required=True,
            doc="Timezone used for aggregation (IANA format)"
        ),
    )


def get_health_data_daily_agg_partition_spec() -> PartitionSpec:
    """
    Get partition specification for health_data_daily_agg table
    
    Partitions by:
    - user_id: User partitions
    - aggregation_date: Daily partitions
    - data_type: Sub-partitions by health data type
    
    Returns:
        Iceberg PartitionSpec
    """
    return PartitionSpec(
        PartitionField(
            source_id=1,  # user_id field
            field_id=1000,
            transform=IdentityTransform(),
            name="user_id"
        ),
        PartitionField(
            source_id=3,  # aggregation_date field
            field_id=1001,
            transform=IdentityTransform(),
            name="aggregation_date"
        ),
        PartitionField(
            source_id=2,  # data_type field
            field_id=1002,
            transform=IdentityTransform(),
            name="data_type"
        ),
    )


def get_health_data_daily_agg_properties() -> dict:
    """
    Get table properties for health_data_daily_agg table
    
    Returns:
        Dictionary of table properties
    """
    return {
        "write.format.default": "parquet",
        "write.parquet.compression-codec": "snappy",
        "write.metadata.compression-codec": "gzip",
        "write.upsert.enabled": "true",
        "commit.retry.num-retries": "3",
        "commit.retry.min-wait-ms": "100",
    }


def get_health_data_weekly_agg_schema() -> Schema:
    """
    Get schema for health_data_weekly_agg table
    
    Weekly aggregated statistics for health data.
    
    Returns:
        Iceberg Schema for weekly aggregates
    """
    return Schema(
        NestedField(
            field_id=1,
            name="user_id",
            field_type=StringType(),
            required=True,
            doc="User identifier"
        ),
        NestedField(
            field_id=2,
            name="data_type",
            field_type=StringType(),
            required=True,
            doc="Health data type (heartRate, steps, etc.)"
        ),
        NestedField(
            field_id=3,
            name="week_start_date",
            field_type=StringType(),  # DATE as string in format YYYY-MM-DD
            required=True,
            doc="Week start date (Monday, YYYY-MM-DD)"
        ),
        NestedField(
            field_id=4,
            name="week_end_date",
            field_type=StringType(),  # DATE as string in format YYYY-MM-DD
            required=True,
            doc="Week end date (Sunday, YYYY-MM-DD)"
        ),
        NestedField(
            field_id=5,
            name="year",
            field_type=LongType(),
            required=True,
            doc="Year"
        ),
        NestedField(
            field_id=6,
            name="week_of_year",
            field_type=LongType(),
            required=True,
            doc="ISO week number (1-53)"
        ),
        NestedField(
            field_id=7,
            name="window_start",
            field_type=TimestampType(),
            required=True,
            doc="Window start timestamp"
        ),
        NestedField(
            field_id=8,
            name="window_end",
            field_type=TimestampType(),
            required=True,
            doc="Window end timestamp"
        ),
        NestedField(
            field_id=9,
            name="min_value",
            field_type=DoubleType(),
            required=True,
            doc="Minimum value in window"
        ),
        NestedField(
            field_id=10,
            name="max_value",
            field_type=DoubleType(),
            required=True,
            doc="Maximum value in window"
        ),
        NestedField(
            field_id=11,
            name="avg_value",
            field_type=DoubleType(),
            required=True,
            doc="Average value in window"
        ),
        NestedField(
            field_id=12,
            name="sum_value",
            field_type=DoubleType(),
            required=True,
            doc="Sum of values in window"
        ),
        NestedField(
            field_id=13,
            name="count",
            field_type=LongType(),
            required=True,
            doc="Count of values in window"
        ),
        NestedField(
            field_id=14,
            name="stddev_value",
            field_type=DoubleType(),
            required=True,
            doc="Standard deviation of values"
        ),
        NestedField(
            field_id=15,
            name="daily_avg_of_avg",
            field_type=DoubleType(),
            required=False,
            doc="Average of daily averages"
        ),
        NestedField(
            field_id=16,
            name="record_count",
            field_type=LongType(),
            required=True,
            doc="Number of records aggregated"
        ),
        NestedField(
            field_id=17,
            name="updated_at",
            field_type=TimestampType(),
            required=True,
            doc="Last update timestamp"
        ),
        NestedField(
            field_id=18,
            name="timezone",
            field_type=StringType(),
            required=True,
            doc="Timezone used for aggregation (IANA format)"
        ),
    )


def get_health_data_weekly_agg_partition_spec() -> PartitionSpec:
    """
    Get partition specification for health_data_weekly_agg table
    
    Partitions by:
    - user_id: User partitions
    - year: Year partitions
    - week_of_year: Week number sub-partitions
    - data_type: Health data type sub-partitions
    
    Returns:
        Iceberg PartitionSpec
    """
    return PartitionSpec(
        PartitionField(
            source_id=1,  # user_id field
            field_id=1000,
            transform=IdentityTransform(),
            name="user_id"
        ),
        PartitionField(
            source_id=5,  # year field
            field_id=1001,
            transform=IdentityTransform(),
            name="year"
        ),
        PartitionField(
            source_id=6,  # week_of_year field
            field_id=1002,
            transform=IdentityTransform(),
            name="week_of_year"
        ),
        PartitionField(
            source_id=2,  # data_type field
            field_id=1003,
            transform=IdentityTransform(),
            name="data_type"
        ),
    )


def get_health_data_weekly_agg_properties() -> dict:
    """
    Get table properties for health_data_weekly_agg table
    
    Returns:
        Dictionary of table properties
    """
    return {
        "write.format.default": "parquet",
        "write.parquet.compression-codec": "snappy",
        "write.metadata.compression-codec": "gzip",
        "write.upsert.enabled": "true",
        "commit.retry.num-retries": "3",
        "commit.retry.min-wait-ms": "100",
    }


def get_health_data_monthly_agg_schema() -> Schema:
    """
    Get schema for health_data_monthly_agg table
    
    Monthly aggregated statistics for health data.
    
    Returns:
        Iceberg Schema for monthly aggregates
    """
    return Schema(
        NestedField(
            field_id=1,
            name="user_id",
            field_type=StringType(),
            required=True,
            doc="User identifier"
        ),
        NestedField(
            field_id=2,
            name="data_type",
            field_type=StringType(),
            required=True,
            doc="Health data type (heartRate, steps, etc.)"
        ),
        NestedField(
            field_id=3,
            name="year",
            field_type=LongType(),
            required=True,
            doc="Year"
        ),
        NestedField(
            field_id=4,
            name="month",
            field_type=LongType(),
            required=True,
            doc="Month (1-12)"
        ),
        NestedField(
            field_id=5,
            name="month_start_date",
            field_type=StringType(),  # DATE as string in format YYYY-MM-DD
            required=True,
            doc="Month start date (1st day, YYYY-MM-DD)"
        ),
        NestedField(
            field_id=6,
            name="month_end_date",
            field_type=StringType(),  # DATE as string in format YYYY-MM-DD
            required=True,
            doc="Month end date (last day, YYYY-MM-DD)"
        ),
        NestedField(
            field_id=7,
            name="window_start",
            field_type=TimestampType(),
            required=True,
            doc="Window start timestamp"
        ),
        NestedField(
            field_id=8,
            name="window_end",
            field_type=TimestampType(),
            required=True,
            doc="Window end timestamp"
        ),
        NestedField(
            field_id=9,
            name="min_value",
            field_type=DoubleType(),
            required=True,
            doc="Minimum value in window"
        ),
        NestedField(
            field_id=10,
            name="max_value",
            field_type=DoubleType(),
            required=True,
            doc="Maximum value in window"
        ),
        NestedField(
            field_id=11,
            name="avg_value",
            field_type=DoubleType(),
            required=True,
            doc="Average value in window"
        ),
        NestedField(
            field_id=12,
            name="sum_value",
            field_type=DoubleType(),
            required=True,
            doc="Sum of values in window"
        ),
        NestedField(
            field_id=13,
            name="count",
            field_type=LongType(),
            required=True,
            doc="Count of values in window"
        ),
        NestedField(
            field_id=14,
            name="stddev_value",
            field_type=DoubleType(),
            required=True,
            doc="Standard deviation of values"
        ),
        NestedField(
            field_id=15,
            name="daily_avg_of_avg",
            field_type=DoubleType(),
            required=False,
            doc="Average of daily averages"
        ),
        NestedField(
            field_id=16,
            name="record_count",
            field_type=LongType(),
            required=True,
            doc="Number of records aggregated"
        ),
        NestedField(
            field_id=17,
            name="updated_at",
            field_type=TimestampType(),
            required=True,
            doc="Last update timestamp"
        ),
        NestedField(
            field_id=18,
            name="timezone",
            field_type=StringType(),
            required=True,
            doc="Timezone used for aggregation (IANA format)"
        ),
    )


def get_health_data_monthly_agg_partition_spec() -> PartitionSpec:
    """
    Get partition specification for health_data_monthly_agg table
    
    Partitions by:
    - user_id: User partitions
    - year: Year partitions
    - month: Month sub-partitions
    - data_type: Health data type sub-partitions
    
    Returns:
        Iceberg PartitionSpec
    """
    return PartitionSpec(
        PartitionField(
            source_id=1,  # user_id field
            field_id=1000,
            transform=IdentityTransform(),
            name="user_id"
        ),
        PartitionField(
            source_id=3,  # year field
            field_id=1001,
            transform=IdentityTransform(),
            name="year"
        ),
        PartitionField(
            source_id=4,  # month field
            field_id=1002,
            transform=IdentityTransform(),
            name="month"
        ),
        PartitionField(
            source_id=2,  # data_type field
            field_id=1003,
            transform=IdentityTransform(),
            name="data_type"
        ),
    )


def get_health_data_monthly_agg_properties() -> dict:
    """
    Get table properties for health_data_monthly_agg table
    
    Returns:
        Dictionary of table properties
    """
    return {
        "write.format.default": "parquet",
        "write.parquet.compression-codec": "snappy",
        "write.metadata.compression-codec": "gzip",
        "write.upsert.enabled": "true",
        "commit.retry.num-retries": "3",
        "commit.retry.min-wait-ms": "100",
    }
