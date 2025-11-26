# Iceberg Catalog and Table Setup

This document describes the Iceberg catalog configuration and table management for the Flink consumer application.

## Overview

The application uses Apache Iceberg as the data lake format with the following components:

- **Catalog Type**: REST Catalog
- **Storage**: MinIO (S3-compatible object storage)
- **Tables**: 
  - `health_data_raw`: Main table for processed health data
  - `health_data_errors`: Dead letter queue (DLQ) for error records
  - `health_data_daily_agg`: Daily aggregated statistics
  - `health_data_weekly_agg`: Weekly aggregated statistics
  - `health_data_monthly_agg`: Monthly aggregated statistics

## Architecture

```
┌─────────────────────┐
│  Flink Application  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Iceberg REST API   │
│   (Port 8181)       │
└──────────┬──────────┘
           │
           ├──────────────────┐
           ▼                  ▼
┌─────────────────┐  ┌──────────────────┐
│  Catalog        │  │  MinIO Storage   │
│  Metadata       │  │  (Port 9000)     │
└─────────────────┘  └──────────────────┘
```

## Configuration

### Environment Variables

```bash
# Iceberg REST Catalog
ICEBERG_CATALOG_TYPE=rest
ICEBERG_CATALOG_NAME=health_catalog
ICEBERG_CATALOG_URI=http://localhost:8181
ICEBERG_WAREHOUSE=s3a://data-lake/warehouse
ICEBERG_DATABASE=health_db
ICEBERG_TABLE_RAW=health_data_raw
ICEBERG_TABLE_ERRORS=health_data_errors

# MinIO Storage
S3_ENDPOINT=http://localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_PATH_STYLE_ACCESS=true
S3_BUCKET=data-lake
```

### Catalog Configuration

The `IcebergCatalog` class manages the connection to the REST catalog:

```python
from flink_consumer.config.settings import Settings
from flink_consumer.iceberg.catalog import IcebergCatalog

settings = Settings()
catalog = IcebergCatalog(settings)
catalog.initialize()
```

## Table Schemas

### health_data_raw Table

Main table for storing processed health data records.

**Schema:**
- `device_id` (STRING, required): Device identifier
- `user_id` (STRING, required): User identifier
- `sample_id` (STRING, required): Sample identifier
- `data_type` (STRING, required): Health data type (heartRate, steps, etc.)
- `value` (DOUBLE, required): Measurement value
- `unit` (STRING, required): Unit of measurement
- `start_date` (TIMESTAMP, required): Start timestamp of measurement
- `end_date` (TIMESTAMP, required): End timestamp of measurement
- `source_bundle` (STRING, optional): Source bundle identifier
- `metadata` (MAP<STRING, STRING>, optional): Additional metadata
- `is_synced` (BOOLEAN, required): Sync status flag
- `created_at` (TIMESTAMP, required): Record creation timestamp
- `payload_timestamp` (TIMESTAMP, required): Original payload timestamp
- `app_version` (STRING, required): Application version
- `processing_time` (TIMESTAMP, required): Flink processing timestamp

**Partitioning:**
- `days(start_date)`: Daily partitions based on measurement start date
- `data_type`: Sub-partitions by health data type

**Properties:**
- Format: Parquet with Snappy compression
- Target file size: 256MB
- Row group size: 128MB
- Page size: 1MB

### health_data_errors Table

Dead letter queue for storing error records.

**Schema:**
- `error_id` (STRING, required): Error record identifier
- `error_type` (STRING, required): Type of error
- `error_message` (STRING, required): Error message description
- `error_timestamp` (TIMESTAMP, required): When the error occurred
- `raw_payload` (STRING, optional): Original raw payload
- `user_id` (STRING, optional): User identifier if available
- `device_id` (STRING, optional): Device identifier if available
- `sample_id` (STRING, optional): Sample identifier if available
- `data_type` (STRING, optional): Health data type if available
- `kafka_topic` (STRING, required): Source Kafka topic
- `kafka_partition` (LONG, required): Source Kafka partition
- `kafka_offset` (LONG, required): Source Kafka offset

**Partitioning:**
- `days(error_timestamp)`: Daily partitions based on error timestamp
- `error_type`: Sub-partitions by error type

### health_data_daily_agg Table

Daily aggregated statistics for health data.

**Schema:**
- `user_id` (STRING, required): User identifier
- `data_type` (STRING, required): Health data type
- `aggregation_date` (STRING, required): Aggregation date (YYYY-MM-DD)
- `window_start` (TIMESTAMP, required): Window start timestamp
- `window_end` (TIMESTAMP, required): Window end timestamp
- `min_value` (DOUBLE, required): Minimum value in window
- `max_value` (DOUBLE, required): Maximum value in window
- `avg_value` (DOUBLE, required): Average value in window
- `sum_value` (DOUBLE, required): Sum of values in window
- `count` (LONG, required): Count of values in window
- `stddev_value` (DOUBLE, required): Standard deviation of values
- `first_value` (DOUBLE, optional): First value in window
- `last_value` (DOUBLE, optional): Last value in window
- `record_count` (LONG, required): Number of records aggregated
- `updated_at` (TIMESTAMP, required): Last update timestamp

**Partitioning:**
- `user_id`: User partitions
- `aggregation_date`: Daily partitions
- `data_type`: Sub-partitions by health data type

**Properties:**
- Format: Parquet with Snappy compression
- Upsert enabled for late data handling

### health_data_weekly_agg Table

Weekly aggregated statistics for health data.

**Schema:**
- `user_id` (STRING, required): User identifier
- `data_type` (STRING, required): Health data type
- `week_start_date` (STRING, required): Week start date (Monday)
- `week_end_date` (STRING, required): Week end date (Sunday)
- `year` (LONG, required): Year
- `week_of_year` (LONG, required): ISO week number (1-53)
- `window_start` (TIMESTAMP, required): Window start timestamp
- `window_end` (TIMESTAMP, required): Window end timestamp
- `min_value` (DOUBLE, required): Minimum value in window
- `max_value` (DOUBLE, required): Maximum value in window
- `avg_value` (DOUBLE, required): Average value in window
- `sum_value` (DOUBLE, required): Sum of values in window
- `count` (LONG, required): Count of values in window
- `stddev_value` (DOUBLE, required): Standard deviation of values
- `daily_avg_of_avg` (DOUBLE, optional): Average of daily averages
- `record_count` (LONG, required): Number of records aggregated
- `updated_at` (TIMESTAMP, required): Last update timestamp

**Partitioning:**
- `user_id`: User partitions
- `year`: Year partitions
- `week_of_year`: Week number sub-partitions
- `data_type`: Health data type sub-partitions

**Properties:**
- Format: Parquet with Snappy compression
- Upsert enabled for late data handling

### health_data_monthly_agg Table

Monthly aggregated statistics for health data.

**Schema:**
- `user_id` (STRING, required): User identifier
- `data_type` (STRING, required): Health data type
- `year` (LONG, required): Year
- `month` (LONG, required): Month (1-12)
- `month_start_date` (STRING, required): Month start date (1st day)
- `month_end_date` (STRING, required): Month end date (last day)
- `window_start` (TIMESTAMP, required): Window start timestamp
- `window_end` (TIMESTAMP, required): Window end timestamp
- `min_value` (DOUBLE, required): Minimum value in window
- `max_value` (DOUBLE, required): Maximum value in window
- `avg_value` (DOUBLE, required): Average value in window
- `sum_value` (DOUBLE, required): Sum of values in window
- `count` (LONG, required): Count of values in window
- `stddev_value` (DOUBLE, required): Standard deviation of values
- `daily_avg_of_avg` (DOUBLE, optional): Average of daily averages
- `record_count` (LONG, required): Number of records aggregated
- `updated_at` (TIMESTAMP, required): Last update timestamp

**Partitioning:**
- `user_id`: User partitions
- `year`: Year partitions
- `month`: Month sub-partitions
- `data_type`: Health data type sub-partitions

**Properties:**
- Format: Parquet with Snappy compression
- Upsert enabled for late data handling

## Table Management

### Automatic Table Creation

The `IcebergTableManager` class handles automatic table creation:

```python
from flink_consumer.iceberg.table_manager import IcebergTableManager

table_manager = IcebergTableManager(settings, catalog)

# Initialize all tables (namespace + tables)
success = table_manager.initialize_all_tables()
```

### Manual Table Operations

```python
# Check if table exists
exists = table_manager.table_exists("health_data_raw")

# Create specific table
raw_table = table_manager.create_health_data_raw_table()
errors_table = table_manager.create_health_data_errors_table()

# Create aggregation tables
daily_table = table_manager.create_health_data_daily_agg_table()
weekly_table = table_manager.create_health_data_weekly_agg_table()
monthly_table = table_manager.create_health_data_monthly_agg_table()

# Or initialize all aggregation tables at once
success = table_manager.initialize_all_aggregation_tables()

# Load existing table
table = table_manager.get_table("health_data_raw")
```

## Testing

Run the test script to verify the Iceberg setup:

```bash
cd flink_consumer
python examples/test_iceberg_setup.py
```

Expected output:
```
================================================================================
Iceberg Catalog and Table Setup Test
================================================================================

1. Loading configuration...
   Catalog Type: rest
   Catalog Name: health_catalog
   ...

2. Initializing Iceberg REST catalog...
   ✓ Catalog initialized successfully

3. Testing catalog connection...
   ✓ Catalog connection successful

4. Initializing table manager...
   ✓ Table manager initialized

5. Creating namespace: health_db
   ✓ Namespace ready

6. Creating table: health_data_raw
   ✓ Raw data table created/verified
   Location: s3a://data-lake/warehouse/health_db/health_data_raw
   ...

7. Creating table: health_data_errors
   ✓ Errors table created/verified
   ...

8. Verifying all tables...
   ✓ health_data_raw exists
   ✓ health_data_errors exists

9. Creating aggregation tables...
   ✓ All aggregation tables created/verified

10. Verifying aggregation tables...
   ✓ health_data_daily_agg exists
   ✓ health_data_weekly_agg exists
   ✓ health_data_monthly_agg exists

================================================================================
✓ All tests passed successfully!
  - Raw data table: health_data_raw
  - Error table: health_data_errors
  - Daily aggregation table: health_data_daily_agg
  - Weekly aggregation table: health_data_weekly_agg
  - Monthly aggregation table: health_data_monthly_agg
================================================================================
```

## Prerequisites

### Running Iceberg REST Catalog

You need an Iceberg REST catalog server running. You can use:

1. **Tabular REST Catalog** (recommended for production)
2. **Polaris Catalog** (open source)
3. **Custom REST implementation**

Example using Docker:

```bash
docker run -d \
  --name iceberg-rest \
  -p 8181:8181 \
  -e CATALOG_WAREHOUSE=s3a://data-lake/warehouse \
  -e CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO \
  -e CATALOG_S3_ENDPOINT=http://minio:9000 \
  -e CATALOG_S3_ACCESS__KEY__ID=minioadmin \
  -e CATALOG_S3_SECRET__ACCESS__KEY=minioadmin \
  -e CATALOG_S3_PATH__STYLE__ACCESS=true \
  tabulario/iceberg-rest:latest
```

### Running MinIO

```bash
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

Create the bucket:

```bash
# Using MinIO client
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/data-lake
mc mb local/flink-checkpoints
```

## Troubleshooting

### Connection Issues

**Problem**: Cannot connect to REST catalog

**Solution**:
- Verify REST catalog is running: `curl http://localhost:8181/v1/config`
- Check network connectivity
- Verify `ICEBERG_CATALOG_URI` is correct

### MinIO Access Issues

**Problem**: S3 access denied errors

**Solution**:
- Verify MinIO credentials in environment variables
- Check bucket exists: `mc ls local/`
- Ensure `S3_PATH_STYLE_ACCESS=true` for MinIO

### Table Creation Failures

**Problem**: Table creation fails with schema errors

**Solution**:
- Check catalog logs for detailed errors
- Verify namespace exists
- Ensure warehouse path is accessible

## Best Practices

1. **Namespace Management**: Always create namespace before tables
2. **Table Properties**: Use appropriate compression and file sizes for your workload
3. **Partitioning**: Choose partition strategy based on query patterns
4. **Monitoring**: Monitor table metadata size and snapshot count
5. **Maintenance**: Regularly expire old snapshots and compact small files

## References

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [PyIceberg Documentation](https://py.iceberg.apache.org/)
- [Iceberg REST Catalog Spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
