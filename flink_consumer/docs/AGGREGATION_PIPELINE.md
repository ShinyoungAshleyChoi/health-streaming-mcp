# 집계 파이프라인 가이드

## 개요

유저별, 헬스데이터 타입별로 일간/주간/월간 집계 데이터를 실시간으로 생성하는 Flink 윈도우 기반 파이프라인입니다.

## 집계 테이블 구조

### 1. 일간 집계 (health_data_daily_agg)

**용도**: 유저별 일일 건강 데이터 통계

**스키마**:
```sql
CREATE TABLE health_catalog.health_db.health_data_daily_agg (
    user_id STRING,
    data_type STRING,
    aggregation_date DATE,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    min_value DOUBLE,
    max_value DOUBLE,
    avg_value DOUBLE,
    sum_value DOUBLE,
    count BIGINT,
    stddev_value DOUBLE,
    first_value DOUBLE,
    last_value DOUBLE,
    record_count BIGINT,
    updated_at TIMESTAMP,
    PRIMARY KEY (user_id, data_type, aggregation_date)
)
PARTITIONED BY (aggregation_date, data_type);
```

**쿼리 예제**:
```sql
-- 특정 유저의 최근 7일 심박수 평균
SELECT 
    aggregation_date,
    avg_value as avg_heart_rate,
    min_value as min_heart_rate,
    max_value as max_heart_rate
FROM health_data_daily_agg
WHERE user_id = 'user-123'
  AND data_type = 'heartRate'
  AND aggregation_date >= CURRENT_DATE - INTERVAL '7' DAY
ORDER BY aggregation_date DESC;
```

### 2. 주간 집계 (health_data_weekly_agg)

**용도**: 유저별 주간 건강 데이터 통계 (월요일 시작)

**스키마**:
```sql
CREATE TABLE health_catalog.health_db.health_data_weekly_agg (
    user_id STRING,
    data_type STRING,
    week_start_date DATE,
    week_end_date DATE,
    year INT,
    week_of_year INT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    min_value DOUBLE,
    max_value DOUBLE,
    avg_value DOUBLE,
    sum_value DOUBLE,
    count BIGINT,
    stddev_value DOUBLE,
    daily_avg_of_avg DOUBLE,
    record_count BIGINT,
    updated_at TIMESTAMP,
    PRIMARY KEY (user_id, data_type, year, week_of_year)
)
PARTITIONED BY (year, week_of_year, data_type);
```

**쿼리 예제**:
```sql
-- 특정 유저의 최근 4주 걸음 수 추이
SELECT 
    week_start_date,
    week_end_date,
    sum_value as total_steps,
    avg_value as avg_daily_steps
FROM health_data_weekly_agg
WHERE user_id = 'user-123'
  AND data_type = 'steps'
  AND year = 2025
ORDER BY week_of_year DESC
LIMIT 4;
```

### 3. 월간 집계 (health_data_monthly_agg)

**용도**: 유저별 월간 건강 데이터 통계

**스키마**:
```sql
CREATE TABLE health_catalog.health_db.health_data_monthly_agg (
    user_id STRING,
    data_type STRING,
    year INT,
    month INT,
    month_start_date DATE,
    month_end_date DATE,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    min_value DOUBLE,
    max_value DOUBLE,
    avg_value DOUBLE,
    sum_value DOUBLE,
    count BIGINT,
    stddev_value DOUBLE,
    daily_avg_of_avg DOUBLE,
    record_count BIGINT,
    updated_at TIMESTAMP,
    PRIMARY KEY (user_id, data_type, year, month)
)
PARTITIONED BY (year, month, data_type);
```

**쿼리 예제**:
```sql
-- 특정 유저의 연간 수면 시간 추이
SELECT 
    year,
    month,
    avg_value as avg_sleep_hours,
    sum_value as total_sleep_hours
FROM health_data_monthly_agg
WHERE user_id = 'user-123'
  AND data_type = 'sleepAnalysis'
  AND year = 2025
ORDER BY month;
```

## 윈도우 설정

### 일간 윈도우
- **크기**: 24시간
- **정렬**: 자정 00:00 UTC
- **지연 허용**: 1시간
- **트리거**: 매일 자정 + 1시간

### 주간 윈도우
- **크기**: 7일
- **정렬**: 월요일 00:00 UTC
- **지연 허용**: 6시간
- **트리거**: 매주 월요일 + 6시간

### 월간 윈도우
- **크기**: 30일
- **정렬**: 매월 1일 00:00 UTC
- **지연 허용**: 12시간
- **트리거**: 매월 1일 + 12시간

## 집계 통계 설명

| 필드 | 설명 | 계산 방식 |
|------|------|----------|
| min_value | 최소값 | 윈도우 내 모든 값 중 최소 |
| max_value | 최대값 | 윈도우 내 모든 값 중 최대 |
| avg_value | 평균값 | sum_value / count |
| sum_value | 합계 | 윈도우 내 모든 값의 합 |
| count | 데이터 포인트 수 | 윈도우 내 레코드 개수 |
| stddev_value | 표준편차 | √(E[X²] - E[X]²) |
| first_value | 첫 번째 값 | 시간순 첫 번째 레코드 값 |
| last_value | 마지막 값 | 시간순 마지막 레코드 값 |
| record_count | 원본 레코드 수 | 집계에 사용된 레코드 수 |
| updated_at | 업데이트 시간 | 집계 계산 완료 시간 |

## 지연 데이터 처리

### Allowed Lateness

지연 도착 데이터를 처리하기 위해 각 윈도우에 allowed lateness를 설정합니다:

```python
# 일간 윈도우 - 1시간 지연 허용
daily_windowed = keyed_stream \
    .window(TumblingEventTimeWindows.of(Time.days(1))) \
    .allowed_lateness(Time.hours(1))

# 주간 윈도우 - 6시간 지연 허용
weekly_windowed = keyed_stream \
    .window(TumblingEventTimeWindows.of(Time.days(7))) \
    .allowed_lateness(Time.hours(6))

# 월간 윈도우 - 12시간 지연 허용
monthly_windowed = keyed_stream \
    .window(TumblingEventTimeWindows.of(Time.days(30))) \
    .allowed_lateness(Time.hours(12))
```

### Upsert 동작

지연 데이터가 도착하면:
1. 해당 윈도우의 집계가 재계산됩니다
2. Iceberg의 Upsert 기능으로 기존 레코드를 업데이트합니다
3. Primary key (user_id, data_type, date/week/month)로 중복을 방지합니다
4. `updated_at` 타임스탬프가 갱신됩니다

## 데이터 타입별 검증 규칙

### 심박수 (heartRate)
- **단위**: count/min
- **유효 범위**: 30 - 250
- **이상치**: < 40 또는 > 200

### 걸음 수 (steps)
- **단위**: count
- **유효 범위**: 0 - 100,000
- **이상치**: > 50,000 (일일)

### 수면 분석 (sleepAnalysis)
- **단위**: hours
- **유효 범위**: 0 - 24
- **이상치**: < 2 또는 > 16

### 혈압 (bloodPressure)
- **단위**: mmHg
- **유효 범위**: 수축기 70-200, 이완기 40-130
- **이상치**: 수축기 > 180 또는 이완기 > 120

## 성능 최적화

### 병렬도 설정

```python
# 집계별 병렬도 차등 설정
daily_agg.set_parallelism(12)    # 높은 빈도 - 높은 병렬도
weekly_agg.set_parallelism(6)    # 중간 빈도 - 중간 병렬도
monthly_agg.set_parallelism(3)   # 낮은 빈도 - 낮은 병렬도
```

### State TTL

```python
# 7일 후 상태 자동 정리
ttl_config = StateTtlConfig \
    .new_builder(Time.days(7)) \
    .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
    .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired) \
    .build()
```

### 메모리 관리

- **윈도우 상태**: RocksDB 사용 (대용량 상태 지원)
- **체크포인트**: 증분 체크포인트 활성화
- **상태 압축**: Snappy 압축 사용

## 모니터링 메트릭

### 윈도우 메트릭
- `windows_processed`: 처리된 윈도우 수
- `records_aggregated`: 집계된 레코드 수
- `window_latency_ms`: 윈도우 처리 지연 시간

### 집계 품질 메트릭
- `valid_aggregations`: 유효한 집계 수
- `invalid_aggregations`: 검증 실패 집계 수
- `late_data_updates`: 지연 데이터로 인한 업데이트 수

### 알림 규칙

```yaml
# Prometheus 알림 예제
- alert: HighWindowLatency
  expr: window_latency_ms > 300000  # 5분 이상
  for: 10m
  annotations:
    summary: "윈도우 처리 지연 발생"

- alert: HighInvalidAggregations
  expr: rate(invalid_aggregations[5m]) > 10
  for: 5m
  annotations:
    summary: "집계 검증 실패율 높음"
```

## 사용 예제

### 대시보드 쿼리

**일일 활동 요약**:
```sql
SELECT 
    d.aggregation_date,
    d.avg_value as avg_heart_rate,
    s.sum_value as total_steps,
    sl.avg_value as avg_sleep_hours
FROM health_data_daily_agg d
LEFT JOIN health_data_daily_agg s 
    ON d.user_id = s.user_id 
    AND d.aggregation_date = s.aggregation_date 
    AND s.data_type = 'steps'
LEFT JOIN health_data_daily_agg sl 
    ON d.user_id = sl.user_id 
    AND d.aggregation_date = sl.aggregation_date 
    AND sl.data_type = 'sleepAnalysis'
WHERE d.user_id = 'user-123'
  AND d.data_type = 'heartRate'
  AND d.aggregation_date >= CURRENT_DATE - INTERVAL '30' DAY
ORDER BY d.aggregation_date DESC;
```

**주간 트렌드 분석**:
```sql
SELECT 
    week_start_date,
    data_type,
    avg_value,
    LAG(avg_value) OVER (PARTITION BY data_type ORDER BY week_start_date) as prev_week_avg,
    (avg_value - LAG(avg_value) OVER (PARTITION BY data_type ORDER BY week_start_date)) / 
        LAG(avg_value) OVER (PARTITION BY data_type ORDER BY week_start_date) * 100 as pct_change
FROM health_data_weekly_agg
WHERE user_id = 'user-123'
  AND year = 2025
ORDER BY week_start_date DESC, data_type;
```

**월간 비교**:
```sql
SELECT 
    year,
    month,
    data_type,
    avg_value as current_month_avg,
    LAG(avg_value, 1) OVER (PARTITION BY data_type ORDER BY year, month) as prev_month_avg,
    LAG(avg_value, 12) OVER (PARTITION BY data_type ORDER BY year, month) as same_month_last_year
FROM health_data_monthly_agg
WHERE user_id = 'user-123'
ORDER BY year DESC, month DESC, data_type;
```

## 트러블슈팅

### 문제: 집계 데이터가 생성되지 않음

**원인**:
- 워터마크가 진행되지 않음
- 윈도우가 트리거되지 않음

**해결**:
```bash
# 워터마크 메트릭 확인
curl http://flink-jobmanager:8081/jobs/<job-id>/metrics?get=currentInputWatermark

# 데이터 스트림에 타임스탬프가 올바르게 할당되었는지 확인
# start_date 필드가 존재하고 유효한지 검증
```

### 문제: 지연 데이터가 반영되지 않음

**원인**:
- Allowed lateness 시간 초과
- Upsert 설정 누락

**해결**:
```python
# Allowed lateness 증가
.allowed_lateness(Time.hours(2))  # 1시간 → 2시간

# Iceberg 테이블 Upsert 활성화 확인
TBLPROPERTIES ('write.upsert.enabled' = 'true')
```

### 문제: 메모리 부족 오류

**원인**:
- 윈도우 상태가 너무 큼
- State TTL 미설정

**해결**:
```python
# State TTL 설정
ttl_config = StateTtlConfig.new_builder(Time.days(7)).build()

# RocksDB 메모리 튜닝
state.backend.rocksdb.memory.managed: true
state.backend.rocksdb.memory.write-buffer-ratio: 0.5
```

## 다음 단계

1. **Task 12 구현**: 집계 파이프라인 코드 작성
2. **테스트**: 윈도우 동작 및 집계 정확성 검증
3. **모니터링**: Grafana 대시보드 구성
4. **최적화**: 병렬도 및 리소스 튜닝
