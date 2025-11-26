# Aggregation Tables

## Overview

집계 테이블은 실시간 스트림 데이터를 일간, 주간, 월간 단위로 집계하여 저장합니다. 이를 통해 대시보드와 분석 쿼리의 성능을 크게 향상시킬 수 있습니다.

## Table Structure

### 1. health_data_daily_agg (일간 집계)

**용도**: 하루 단위 건강 데이터 통계

**주요 필드**:
- `user_id`, `data_type`: 집계 키
- `aggregation_date`: 집계 날짜 (YYYY-MM-DD)
- `min_value`, `max_value`, `avg_value`: 기본 통계
- `sum_value`, `count`: 합계 및 개수
- `stddev_value`: 표준편차
- `first_value`, `last_value`: 첫/마지막 값
- `record_count`: 집계된 레코드 수

**파티셔닝**:
```
user_id / aggregation_date / data_type
```

**예시 쿼리**:
```sql
-- 특정 사용자의 2024년 1월 심박수 일간 통계
SELECT 
    aggregation_date,
    min_value,
    max_value,
    avg_value,
    stddev_value
FROM health_data_daily_agg
WHERE user_id = 'user123'
  AND data_type = 'heartRate'
  AND aggregation_date >= '2024-01-01'
  AND aggregation_date < '2024-02-01'
ORDER BY aggregation_date;
```

### 2. health_data_weekly_agg (주간 집계)

**용도**: 주 단위 건강 데이터 통계 (월요일 시작)

**주요 필드**:
- `user_id`, `data_type`: 집계 키
- `year`, `week_of_year`: 연도 및 주차 (ISO 8601)
- `week_start_date`, `week_end_date`: 주 시작/종료일
- `min_value`, `max_value`, `avg_value`: 기본 통계
- `daily_avg_of_avg`: 일간 평균의 평균
- `record_count`: 집계된 레코드 수

**파티셔닝**:
```
user_id / year / week_of_year / data_type
```

**예시 쿼리**:
```sql
-- 특정 사용자의 2024년 걸음 수 주간 통계
SELECT 
    year,
    week_of_year,
    week_start_date,
    week_end_date,
    sum_value as total_steps,
    avg_value as avg_daily_steps
FROM health_data_weekly_agg
WHERE user_id = 'user123'
  AND data_type = 'steps'
  AND year = 2024
ORDER BY year, week_of_year;
```

### 3. health_data_monthly_agg (월간 집계)

**용도**: 월 단위 건강 데이터 통계

**주요 필드**:
- `user_id`, `data_type`: 집계 키
- `year`, `month`: 연도 및 월 (1-12)
- `month_start_date`, `month_end_date`: 월 시작/종료일
- `min_value`, `max_value`, `avg_value`: 기본 통계
- `daily_avg_of_avg`: 일간 평균의 평균
- `record_count`: 집계된 레코드 수

**파티셔닝**:
```
user_id / year / month / data_type
```

**예시 쿼리**:
```sql
-- 특정 사용자의 2024년 심박수 월간 통계
SELECT 
    year,
    month,
    month_start_date,
    month_end_date,
    min_value,
    max_value,
    avg_value,
    stddev_value
FROM health_data_monthly_agg
WHERE user_id = 'user123'
  AND data_type = 'heartRate'
  AND year = 2024
ORDER BY year, month;
```

## Table Creation

### Automatic Creation

```python
from flink_consumer.config.settings import Settings
from flink_consumer.iceberg.catalog import IcebergCatalog
from flink_consumer.iceberg.table_manager import IcebergTableManager

settings = Settings()
catalog = IcebergCatalog(settings)
table_manager = IcebergTableManager(settings, catalog)

# Create all aggregation tables
success = table_manager.initialize_all_aggregation_tables()
```

### Individual Table Creation

```python
# Create daily aggregation table
daily_table = table_manager.create_health_data_daily_agg_table()

# Create weekly aggregation table
weekly_table = table_manager.create_health_data_weekly_agg_table()

# Create monthly aggregation table
monthly_table = table_manager.create_health_data_monthly_agg_table()
```

## Data Flow

```
Raw Data Stream
      │
      ▼
┌─────────────────┐
│  Windowing      │
│  (Tumbling)     │
└─────────────────┘
      │
      ├──────────────────┬──────────────────┐
      ▼                  ▼                  ▼
┌──────────┐      ┌──────────┐      ┌──────────┐
│  Daily   │      │  Weekly  │      │ Monthly  │
│  Window  │      │  Window  │      │  Window  │
│  (24h)   │      │  (7d)    │      │  (30d)   │
└──────────┘      └──────────┘      └──────────┘
      │                  │                  │
      ▼                  ▼                  ▼
┌──────────┐      ┌──────────┐      ┌──────────┐
│Aggregate │      │Aggregate │      │Aggregate │
│ Function │      │ Function │      │ Function │
└──────────┘      └──────────┘      └──────────┘
      │                  │                  │
      ▼                  ▼                  ▼
┌──────────┐      ┌──────────┐      ┌──────────┐
│  daily   │      │  weekly  │      │ monthly  │
│  _agg    │      │  _agg    │      │  _agg    │
└──────────┘      └──────────┘      └──────────┘
```

## Upsert Support

모든 집계 테이블은 upsert를 지원합니다:

```python
properties = {
    "write.upsert.enabled": "true"
}
```

이를 통해:
- **Late data handling**: 늦게 도착한 데이터로 기존 집계 업데이트
- **Reprocessing**: 데이터 재처리 시 중복 방지
- **Corrections**: 잘못된 데이터 수정

## Performance Optimization

### Partitioning Strategy

각 테이블은 쿼리 패턴에 최적화된 파티셔닝을 사용합니다:

- **Daily**: `user_id` + `aggregation_date` + `data_type`
  - 사용자별 날짜 범위 쿼리에 최적화
  - 사용자 데이터 격리 (GDPR 준수)
  
- **Weekly**: `user_id` + `year` + `week_of_year` + `data_type`
  - 사용자별 주차별 쿼리에 최적화
  - 사용자 데이터 격리
  
- **Monthly**: `user_id` + `year` + `month` + `data_type`
  - 사용자별 월별 쿼리에 최적화
  - 사용자 데이터 격리

### Compression

모든 테이블은 Snappy 압축을 사용합니다:
- 빠른 압축/해제 속도
- 적절한 압축률
- CPU 오버헤드 최소화

## Use Cases

### 1. Dashboard Queries

집계 테이블을 사용하면 대시보드 쿼리가 훨씬 빠릅니다:

```sql
-- Raw 테이블 쿼리 (느림)
SELECT 
    DATE(start_date) as date,
    AVG(value) as avg_value
FROM health_data_raw
WHERE user_id = 'user123'
  AND data_type = 'heartRate'
  AND start_date >= '2024-01-01'
GROUP BY DATE(start_date);

-- Daily agg 테이블 쿼리 (빠름)
SELECT 
    aggregation_date,
    avg_value
FROM health_data_daily_agg
WHERE user_id = 'user123'
  AND data_type = 'heartRate'
  AND aggregation_date >= '2024-01-01';
```

### 2. Trend Analysis

주간/월간 트렌드 분석:

```sql
-- 최근 12주 걸음 수 트렌드
SELECT 
    week_start_date,
    avg_value as avg_daily_steps,
    sum_value as total_weekly_steps
FROM health_data_weekly_agg
WHERE user_id = 'user123'
  AND data_type = 'steps'
  AND year = 2024
ORDER BY year DESC, week_of_year DESC
LIMIT 12;
```

### 3. Comparative Analysis

월별 비교 분석:

```sql
-- 2024년 vs 2023년 월별 심박수 비교
SELECT 
    month,
    MAX(CASE WHEN year = 2024 THEN avg_value END) as avg_2024,
    MAX(CASE WHEN year = 2023 THEN avg_value END) as avg_2023
FROM health_data_monthly_agg
WHERE user_id = 'user123'
  AND data_type = 'heartRate'
  AND year IN (2023, 2024)
GROUP BY month
ORDER BY month;
```

## Maintenance

### Snapshot Management

집계 테이블도 정기적인 스냅샷 관리가 필요합니다:

```python
# Expire old snapshots (keep last 30 days)
table.expire_snapshots(
    older_than=datetime.now() - timedelta(days=30)
)
```

### Compaction

작은 파일들을 주기적으로 압축:

```python
# Compact small files
table.rewrite_data_files(
    target_file_size_bytes=268435456  # 256MB
)
```

## Testing

테스트 스크립트 실행:

```bash
cd flink_consumer
python examples/test_iceberg_setup.py
```

집계 테이블이 정상적으로 생성되었는지 확인합니다.

## References

- [Iceberg Upsert Documentation](https://iceberg.apache.org/docs/latest/spark-writes/#upsert)
- [Partitioning Best Practices](https://iceberg.apache.org/docs/latest/partitioning/)
- [Performance Tuning](https://iceberg.apache.org/docs/latest/performance/)
