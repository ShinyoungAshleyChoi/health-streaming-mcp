"""
Iceberg table schemas for aggregated health data.

This module defines the schemas for daily, weekly, and monthly aggregation tables
with support for upsert operations to handle late-arriving data.
"""

from dataclasses import dataclass
from typing import Optional
from datetime import date, datetime


@dataclass
class DailyAggregateSchema:
    """
    Schema for daily health data aggregations.
    
    Aggregates health data by user_id and data_type on a daily basis.
    Primary key: (user_id, data_type, aggregation_date)
    Partitioned by: (aggregation_date, data_type)
    """
    user_id: str
    data_type: str
    aggregation_date: date
    window_start: int  # Unix timestamp (ms)
    window_end: int  # Unix timestamp (ms)
    min_value: float
    max_value: float
    avg_value: float
    sum_value: float
    count: int
    stddev_value: float
    first_value: Optional[float]
    last_value: Optional[float]
    record_count: int
    updated_at: int  # Unix timestamp (ms)
    
    @staticmethod
    def get_iceberg_ddl(catalog_name: str = 'health_catalog', 
                       database_name: str = 'health_db',
                       table_name: str = 'health_data_daily_agg') -> str:
        """
        Generate Iceberg DDL for daily aggregates table.
        
        Args:
            catalog_name: Iceberg catalog name
            database_name: Database name
            table_name: Table name
            
        Returns:
            SQL DDL statement
        """
        return f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{database_name}.{table_name} (
    user_id STRING NOT NULL,
    data_type STRING NOT NULL,
    aggregation_date DATE NOT NULL,
    window_start BIGINT NOT NULL,
    window_end BIGINT NOT NULL,
    min_value DOUBLE NOT NULL,
    max_value DOUBLE NOT NULL,
    avg_value DOUBLE NOT NULL,
    sum_value DOUBLE NOT NULL,
    count BIGINT NOT NULL,
    stddev_value DOUBLE NOT NULL,
    first_value DOUBLE,
    last_value DOUBLE,
    record_count BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (user_id, data_type, aggregation_date) NOT ENFORCED
)
PARTITIONED BY (aggregation_date, data_type)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.metadata.compression-codec' = 'gzip',
    'write.upsert.enabled' = 'true',
    'write.merge.mode' = 'merge-on-read'
);
"""


@dataclass
class WeeklyAggregateSchema:
    """
    Schema for weekly health data aggregations.
    
    Aggregates health data by user_id and data_type on a weekly basis.
    Primary key: (user_id, data_type, year, week_of_year)
    Partitioned by: (year, week_of_year, data_type)
    """
    user_id: str
    data_type: str
    week_start_date: date
    week_end_date: date
    year: int
    week_of_year: int
    window_start: int  # Unix timestamp (ms)
    window_end: int  # Unix timestamp (ms)
    min_value: float
    max_value: float
    avg_value: float
    sum_value: float
    count: int
    stddev_value: float
    daily_avg_of_avg: Optional[float]
    record_count: int
    updated_at: int  # Unix timestamp (ms)
    
    @staticmethod
    def get_iceberg_ddl(catalog_name: str = 'health_catalog',
                       database_name: str = 'health_db',
                       table_name: str = 'health_data_weekly_agg') -> str:
        """
        Generate Iceberg DDL for weekly aggregates table.
        
        Args:
            catalog_name: Iceberg catalog name
            database_name: Database name
            table_name: Table name
            
        Returns:
            SQL DDL statement
        """
        return f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{database_name}.{table_name} (
    user_id STRING NOT NULL,
    data_type STRING NOT NULL,
    week_start_date DATE NOT NULL,
    week_end_date DATE NOT NULL,
    year INT NOT NULL,
    week_of_year INT NOT NULL,
    window_start BIGINT NOT NULL,
    window_end BIGINT NOT NULL,
    min_value DOUBLE NOT NULL,
    max_value DOUBLE NOT NULL,
    avg_value DOUBLE NOT NULL,
    sum_value DOUBLE NOT NULL,
    count BIGINT NOT NULL,
    stddev_value DOUBLE NOT NULL,
    daily_avg_of_avg DOUBLE,
    record_count BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (user_id, data_type, year, week_of_year) NOT ENFORCED
)
PARTITIONED BY (year, week_of_year, data_type)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.metadata.compression-codec' = 'gzip',
    'write.upsert.enabled' = 'true',
    'write.merge.mode' = 'merge-on-read'
);
"""


@dataclass
class MonthlyAggregateSchema:
    """
    Schema for monthly health data aggregations.
    
    Aggregates health data by user_id and data_type on a monthly basis.
    Primary key: (user_id, data_type, year, month)
    Partitioned by: (year, month, data_type)
    """
    user_id: str
    data_type: str
    year: int
    month: int
    month_start_date: date
    month_end_date: date
    window_start: int  # Unix timestamp (ms)
    window_end: int  # Unix timestamp (ms)
    min_value: float
    max_value: float
    avg_value: float
    sum_value: float
    count: int
    stddev_value: float
    daily_avg_of_avg: Optional[float]
    record_count: int
    updated_at: int  # Unix timestamp (ms)
    
    @staticmethod
    def get_iceberg_ddl(catalog_name: str = 'health_catalog',
                       database_name: str = 'health_db',
                       table_name: str = 'health_data_monthly_agg') -> str:
        """
        Generate Iceberg DDL for monthly aggregates table.
        
        Args:
            catalog_name: Iceberg catalog name
            database_name: Database name
            table_name: Table name
            
        Returns:
            SQL DDL statement
        """
        return f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{database_name}.{table_name} (
    user_id STRING NOT NULL,
    data_type STRING NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    month_start_date DATE NOT NULL,
    month_end_date DATE NOT NULL,
    window_start BIGINT NOT NULL,
    window_end BIGINT NOT NULL,
    min_value DOUBLE NOT NULL,
    max_value DOUBLE NOT NULL,
    avg_value DOUBLE NOT NULL,
    sum_value DOUBLE NOT NULL,
    count BIGINT NOT NULL,
    stddev_value DOUBLE NOT NULL,
    daily_avg_of_avg DOUBLE,
    record_count BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (user_id, data_type, year, month) NOT ENFORCED
)
PARTITIONED BY (year, month, data_type)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.metadata.compression-codec' = 'gzip',
    'write.upsert.enabled' = 'true',
    'write.merge.mode' = 'merge-on-read'
);
"""
