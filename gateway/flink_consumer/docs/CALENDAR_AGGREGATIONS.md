# Calendar-Based Aggregations

## 문제점: 고정 기간 윈도우의 한계

### 현재 구현 (Fixed-Duration Windows)

```python
# 문제 1: 고정 30일 월간 윈도우
MONTHLY = {
    'size_days': 30,  # ❌ 항상 30일
}

# 문제 2: UTC 기준 고정
align_to_midnight_utc(timestamp)  # ❌ 모든 사용자가 UTC 기준
```

### 실제 문제 사례

#### 1. 월별 집계 오류

```
2월 집계:
- 실제: 28일 (윤년 29일)
- 고정 윈도우: 30일
- 결과: 3월 1-2일 데이터가 2월에 포함됨 ❌

1월 집계:
- 실제: 31일
- 고정 윈도우: 30일
- 결과: 1월 31일 데이터가 누락됨 ❌
```

#### 2. 타임존 문제

```
한국 사용자 (UTC+9):
- 사용자의 "11월 17일": 2025-11-17 00:00 KST = 2025-11-16 15:00 UTC
- UTC 윈도우: 2025-11-17 00:00 UTC ~ 2025-11-18 00:00 UTC
- 결과: 사용자의 11월 17일 오전 데이터가 11월 16일로 집계됨 ❌

미국 동부 사용자 (UTC-5):
- 사용자의 "11월 17일": 2025-11-17 00:00 EST = 2025-11-17 05:00 UTC
- UTC 윈도우: 2025-11-17 00:00 UTC ~ 2025-11-18 00:00 UTC
- 결과: 사용자의 11월 16일 저녁 데이터가 11월 17일로 집계됨 ❌
```

## 해결책: Calendar-Based Aggregations

### 핵심 아이디어

1. **start_date 기준 집계**: 메시지의 `start_date` 필드를 사용자의 실제 측정 시간으로 간주
2. **실제 달력 기준**: 28-31일의 실제 월 길이 사용
3. **사용자 타임존 지원**: 사용자의 로컬 타임존 기준으로 날짜 계산

### 구현 방법

#### 1. 일간 집계 (Calendar Daily)

```python
from flink_consumer.aggregations.calendar_aggregator import CalendarDailyAggregator

# 한국 타임존 기준 일간 집계
daily_aggregator = CalendarDailyAggregator(user_timezone="Asia/Seoul")

# 스트림에 적용
daily_agg_stream = keyed_stream.process(daily_aggregator)
```

**동작 방식:**
```python
# 레코드
record = {
    'user_id': 'user-123',
    'data_type': 'heartRate',
    'value': 75.0,
    'start_date': 1700179200000,  # 2023-11-17 09:00:00 KST
}

# 한국 타임존 기준 날짜 계산
dt = datetime.fromtimestamp(1700179200000 / 1000, tz=ZoneInfo("Asia/Seoul"))
# → 2023-11-17 09:00:00 KST

aggregation_date = dt.date()
# → 2023-11-17

# 하루 경계
day_start = 2023-11-17 00:00:00 KST
day_end = 2023-11-17 23:59:59 KST

# 결과: 사용자의 실제 11월 17일 데이터로 집계 ✅
```

#### 2. 월간 집계 (Calendar Monthly)

```python
from flink_consumer.aggregations.calendar_aggregator import CalendarMonthlyAggregator

# 실제 달력 월 기준 집계
monthly_aggregator = CalendarMonthlyAggregator(user_timezone="Asia/Seoul")

# 스트림에 적용
monthly_agg_stream = keyed_stream.process(monthly_aggregator)
```

**동작 방식:**
```python
import calendar

# 2월 집계
year, month = 2024, 2
days_in_month = calendar.monthrange(year, month)[1]
# → 29일 (2024년은 윤년)

month_start = 2024-02-01 00:00:00 KST
month_end = 2024-02-29 23:59:59 KST  # ✅ 실제 2월 마지막 날

# 1월 집계
year, month = 2024, 1
days_in_month = calendar.monthrange(year, month)[1]
# → 31일

month_start = 2024-01-01 00:00:00 KST
month_end = 2024-01-31 23:59:59 KST  # ✅ 실제 1월 마지막 날
```

### 비교: Fixed vs Calendar

| 항목 | Fixed-Duration Windows | Calendar-Based Windows |
|------|------------------------|------------------------|
| **일간 집계** | UTC 자정 기준 24시간 | 사용자 타임존 기준 하루 |
| **월간 집계** | 고정 30일 | 실제 달력 월 (28-31일) |
| **타임존** | UTC만 지원 | 사용자 타임존 지원 |
| **정확도** | 타임존 차이로 오류 | 사용자 관점에서 정확 |
| **구현 복잡도** | 낮음 (Flink 내장) | 중간 (커스텀 구현) |
| **성능** | 높음 | 약간 낮음 (상태 관리) |

## 실제 사용 예시

### 파이프라인 구성

```python
from pyflink.datastream import StreamExecutionEnvironment
from flink_consumer.aggregations.calendar_aggregator import (
    CalendarDailyAggregator,
    CalendarMonthlyAggregator
)

env = StreamExecutionEnvironment.get_execution_environment()

# 데이터 스트림
data_stream = env.from_source(kafka_source, ...)

# Watermark 할당 (start_date 기준)
stream_with_watermarks = data_stream.assign_timestamps_and_watermarks(
    WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_minutes(10))
        .with_timestamp_assigner(lambda event: event['start_date'])
)

# Key by (user_id, data_type)
keyed_stream = stream_with_watermarks.key_by(
    lambda row: (row['user_id'], row['data_type'])
)

# 일간 집계 (한국 타임존)
daily_agg = keyed_stream.process(
    CalendarDailyAggregator(user_timezone="Asia/Seoul")
)

# 월간 집계 (한국 타임존)
monthly_agg = keyed_stream.process(
    CalendarMonthlyAggregator(user_timezone="Asia/Seoul")
)

# Iceberg에 저장
daily_agg.sink_to(daily_iceberg_sink)
monthly_agg.sink_to(monthly_iceberg_sink)
```

### 결과 예시

#### 일간 집계 결과

```python
{
    'user_id': 'user-123',
    'data_type': 'heartRate',
    'aggregation_date': '2025-11-17',  # 사용자 타임존 기준 날짜
    'window_start': 1731772800000,     # 2025-11-17 00:00:00 KST
    'window_end': 1731859199999,       # 2025-11-17 23:59:59 KST
    'min_value': 60.0,
    'max_value': 120.0,
    'avg_value': 75.5,
    'sum_value': 7550.0,
    'count': 100,
    'stddev_value': 12.3,
    'first_value': 65.0,
    'last_value': 70.0,
    'record_count': 100,
}
```

#### 월간 집계 결과

```python
{
    'user_id': 'user-123',
    'data_type': 'heartRate',
    'year': 2024,
    'month': 2,
    'month_start_date': '2024-02-01',
    'month_end_date': '2024-02-29',    # ✅ 윤년 2월 = 29일
    'days_in_month': 29,               # ✅ 실제 일수
    'window_start': 1706716800000,     # 2024-02-01 00:00:00 KST
    'window_end': 1709222399999,       # 2024-02-29 23:59:59 KST
    'min_value': 55.0,
    'max_value': 130.0,
    'avg_value': 74.2,
    'sum_value': 215180.0,
    'count': 2900,
    'stddev_value': 13.1,
    'record_count': 2900,
}
```

## 다중 타임존 지원

### 사용자별 타임존 설정

```python
# 사용자 타임존 매핑
user_timezones = {
    'user-kr-001': 'Asia/Seoul',
    'user-us-001': 'America/New_York',
    'user-uk-001': 'Europe/London',
}

# 사용자별 타임존으로 집계
class UserTimezoneAggregator(ProcessFunction):
    def __init__(self):
        self.user_timezones = user_timezones
    
    def process_element(self, record, ctx):
        user_id = record['user_id']
        timezone = self.user_timezones.get(user_id, 'UTC')
        
        # 사용자 타임존으로 날짜 계산
        dt = datetime.fromtimestamp(
            record['start_date'] / 1000,
            tz=ZoneInfo(timezone)
        )
        aggregation_date = dt.date()
        
        # 집계 로직...
```

### 글로벌 서비스 고려사항

1. **타임존 저장**: 사용자 프로필에 타임존 정보 저장
2. **기본 타임존**: 타임존 정보가 없으면 UTC 사용
3. **DST 처리**: `zoneinfo` 라이브러리가 자동으로 처리
4. **성능**: 타임존 변환은 CPU 비용이 낮음

## 마이그레이션 가이드

### 기존 Fixed Windows에서 전환

#### 1단계: 병렬 실행

```python
# 기존 Fixed Windows (유지)
fixed_daily = keyed_stream.window(
    TumblingEventTimeWindows.of(Time.days(1))
).aggregate(...)

# 새로운 Calendar Windows (추가)
calendar_daily = keyed_stream.process(
    CalendarDailyAggregator(user_timezone="Asia/Seoul")
)

# 두 결과를 모두 저장하여 비교
fixed_daily.sink_to(fixed_daily_sink)
calendar_daily.sink_to(calendar_daily_sink)
```

#### 2단계: 검증

```sql
-- 결과 비교 쿼리
SELECT 
    f.aggregation_date,
    f.user_id,
    f.data_type,
    f.avg_value as fixed_avg,
    c.avg_value as calendar_avg,
    ABS(f.avg_value - c.avg_value) as diff
FROM fixed_daily_agg f
JOIN calendar_daily_agg c
    ON f.user_id = c.user_id
    AND f.data_type = c.data_type
    AND f.aggregation_date = c.aggregation_date
WHERE ABS(f.avg_value - c.avg_value) > 1.0
ORDER BY diff DESC;
```

#### 3단계: 전환

```python
# Fixed Windows 제거
# calendar_daily만 사용
calendar_daily = keyed_stream.process(
    CalendarDailyAggregator(user_timezone="Asia/Seoul")
)
calendar_daily.sink_to(daily_sink)
```

## 성능 고려사항

### 상태 크기

Calendar-based aggregations는 Flink 상태를 사용합니다:

```python
# 상태 크기 추정
state_per_key = 200 bytes  # CalendarAggregateState
num_users = 1,000,000
num_data_types = 10
num_active_days = 2  # 현재 날짜 + 지연 데이터

total_state = state_per_key * num_users * num_data_types * num_active_days
# = 200 * 1M * 10 * 2 = 4 GB
```

### 최적화 방법

1. **State TTL 설정**
```python
from pyflink.datastream.state import StateTtlConfig
from pyflink.common import Time

ttl_config = StateTtlConfig \
    .new_builder(Time.days(3)) \
    .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
    .build()
```

2. **RocksDB 사용**
```yaml
state.backend: rocksdb
state.backend.incremental: true
```

3. **Parallelism 조정**
```python
# 일간 집계: 높은 parallelism
daily_agg.set_parallelism(12)

# 월간 집계: 낮은 parallelism
monthly_agg.set_parallelism(3)
```

## 권장사항

### 언제 Calendar-Based를 사용할까?

**사용 권장:**
- ✅ 글로벌 서비스 (다중 타임존)
- ✅ 사용자 대시보드 (사용자 관점의 날짜)
- ✅ 월간 리포트 (정확한 달력 월 필요)
- ✅ 규제 준수 (특정 국가 타임존 기준)

**Fixed Windows 유지:**
- ✅ 내부 모니터링 (UTC 기준으로 충분)
- ✅ 시스템 메트릭 (타임존 무관)
- ✅ 실시간 알림 (정확한 시간 경계 필요)
- ✅ 성능이 중요한 경우

### 하이브리드 접근

```python
# 사용자 대시보드용: Calendar-based
user_daily_agg = keyed_stream.process(
    CalendarDailyAggregator(user_timezone="Asia/Seoul")
)

# 내부 모니터링용: Fixed windows
system_daily_agg = keyed_stream.window(
    TumblingEventTimeWindows.of(Time.days(1))
).aggregate(...)

# 각각 다른 테이블에 저장
user_daily_agg.sink_to(user_facing_table)
system_daily_agg.sink_to(internal_monitoring_table)
```

## 참고 자료

- [Python zoneinfo documentation](https://docs.python.org/3/library/zoneinfo.html)
- [Flink State Management](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/)
- [Flink Process Function](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/process_function/)
