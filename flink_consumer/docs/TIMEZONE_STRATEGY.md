# Timezone Strategy for Calendar Aggregations

## 문제: 현재 메시지에 타임존 정보 없음

### 현재 Payload 구조

```json
{
  "deviceId": "iPhone14-ABC123",
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-11-12T10:30:00Z",
  "appVersion": "1.2.3",
  "samples": [
    {
      "id": "sample-001",
      "type": "heartRate",
      "value": 72,
      "unit": "bpm",
      "startDate": "2025-11-12T10:00:00Z",  // ❌ UTC만 있음
      "endDate": "2025-11-12T10:30:00Z",
      "sourceBundle": "com.apple.health",
      "metadata": {},
      "isSynced": true,
      "createdAt": "2025-11-12T10:30:00Z"
    }
  ]
}
```

**문제점:**
- `startDate`, `endDate`가 UTC (Z suffix)로만 제공됨
- 사용자의 실제 로컬 타임존 정보 없음
- 사용자가 어느 국가/타임존에 있는지 알 수 없음

## 해결 방안

### 방안 1: 메시지에 타임존 필드 추가 (권장)

#### 1.1 Payload 레벨에 타임존 추가

```json
{
  "deviceId": "iPhone14-ABC123",
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-11-12T10:30:00Z",
  "timezone": "Asia/Seoul",  // ✅ 추가
  "appVersion": "1.2.3",
  "samples": [...]
}
```

**장점:**
- 간단하고 명확
- 모든 샘플이 같은 타임존 사용 (일반적인 케이스)
- 기존 필드 수정 불필요

**단점:**
- 샘플별로 다른 타임존 불가능 (여행 중 타임존 변경 케이스)

#### 1.2 Sample 레벨에 타임존 추가

```json
{
  "samples": [
    {
      "id": "sample-001",
      "type": "heartRate",
      "value": 72,
      "startDate": "2025-11-12T10:00:00Z",
      "endDate": "2025-11-12T10:30:00Z",
      "timezone": "Asia/Seoul",  // ✅ 샘플별 타임존
      "sourceBundle": "com.apple.health",
      ...
    }
  ]
}
```

**장점:**
- 샘플별로 다른 타임존 지원 (여행 케이스)
- 더 정확한 데이터

**단점:**
- 메시지 크기 증가
- 구현 복잡도 증가

#### 1.3 ISO 8601 타임존 포함 형식 사용

```json
{
  "samples": [
    {
      "startDate": "2025-11-12T19:00:00+09:00",  // ✅ 타임존 포함
      "endDate": "2025-11-12T19:30:00+09:00",
      ...
    }
  ]
}
```

**장점:**
- 표준 ISO 8601 형식
- 별도 필드 불필요
- 타임존 정보가 타임스탬프에 내장

**단점:**
- 기존 파싱 로직 수정 필요
- IANA 타임존 이름 (Asia/Seoul) 대신 오프셋 (+09:00)만 제공
  - DST (일광절약시간) 처리 어려움

### 방안 2: 사용자 프로필에서 타임존 조회

#### 2.1 사용자 메타데이터 서비스 연동

```python
class UserTimezoneEnricher(AsyncFunction):
    """
    사용자 프로필 서비스에서 타임존 정보 조회
    """
    
    def __init__(self, user_service_url: str):
        self.user_service_url = user_service_url
        self.cache = {}  # 타임존 캐시
    
    async def async_invoke(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        사용자 타임존 조회 및 레코드 enrichment
        """
        user_id = record['user_id']
        
        # 캐시 확인
        if user_id in self.cache:
            timezone = self.cache[user_id]
        else:
            # 사용자 서비스 API 호출
            timezone = await self._fetch_user_timezone(user_id)
            self.cache[user_id] = timezone
        
        record['user_timezone'] = timezone
        return record
    
    async def _fetch_user_timezone(self, user_id: str) -> str:
        """사용자 프로필에서 타임존 조회"""
        import aiohttp
        
        async with aiohttp.ClientSession() as session:
            url = f"{self.user_service_url}/users/{user_id}/timezone"
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('timezone', 'UTC')
                else:
                    return 'UTC'  # 기본값
```

**장점:**
- 메시지 크기 증가 없음
- 중앙화된 사용자 정보 관리
- 타임존 변경 시 한 곳에서만 업데이트

**단점:**
- 외부 서비스 의존성 추가
- 네트워크 레이턴시
- 서비스 장애 시 영향
- 캐시 관리 필요

#### 2.2 사용자 타임존 테이블 조인

```python
# Flink Table API를 사용한 조인
table_env.execute_sql("""
    CREATE TABLE user_timezones (
        user_id STRING,
        timezone STRING,
        updated_at TIMESTAMP,
        PRIMARY KEY (user_id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://db:5432/users',
        'table-name' = 'user_timezones'
    )
""")

# 스트림과 조인
result = table_env.sql_query("""
    SELECT 
        h.*,
        u.timezone as user_timezone
    FROM health_data h
    LEFT JOIN user_timezones FOR SYSTEM_TIME AS OF h.processing_time AS u
        ON h.user_id = u.user_id
""")
```

**장점:**
- Flink의 temporal join 활용
- 캐시 관리 자동화
- 타임존 변경 이력 추적 가능

**단점:**
- 데이터베이스 의존성
- 조인 오버헤드
- 복잡도 증가

### 방안 3: 디바이스 위치 기반 추론

```python
class LocationBasedTimezoneInferrer:
    """
    디바이스 위치 정보로 타임존 추론
    """
    
    def __init__(self):
        from timezonefinder import TimezoneFinder
        self.tf = TimezoneFinder()
    
    def infer_timezone(self, record: Dict[str, Any]) -> str:
        """
        메타데이터의 위치 정보로 타임존 추론
        """
        metadata = record.get('metadata', {})
        
        # GPS 좌표가 있는 경우
        if 'latitude' in metadata and 'longitude' in metadata:
            lat = metadata['latitude']
            lon = metadata['longitude']
            timezone = self.tf.timezone_at(lat=lat, lng=lon)
            return timezone or 'UTC'
        
        # IP 주소 기반 (GeoIP)
        if 'ip_address' in metadata:
            timezone = self._geoip_lookup(metadata['ip_address'])
            return timezone or 'UTC'
        
        # 디바이스 설정 언어/지역 기반 추론
        if 'locale' in metadata:
            timezone = self._locale_to_timezone(metadata['locale'])
            return timezone or 'UTC'
        
        return 'UTC'  # 기본값
```

**장점:**
- 메시지 수정 불필요
- 기존 메타데이터 활용

**단점:**
- 정확도 낮음 (특히 VPN 사용 시)
- 추가 라이브러리 의존성
- 계산 오버헤드

## 권장 구현 전략

### Phase 1: 단기 (메시지 수정 전)

**기본 타임존 사용 + 사용자 프로필 조회**

```python
class HybridTimezoneResolver:
    """
    여러 소스에서 타임존 정보 수집
    """
    
    def __init__(self, default_timezone: str = "UTC"):
        self.default_timezone = default_timezone
        self.user_timezone_cache = {}
    
    def resolve_timezone(self, record: Dict[str, Any]) -> str:
        """
        우선순위에 따라 타임존 결정:
        1. 메시지에 timezone 필드가 있으면 사용
        2. 사용자 프로필 캐시에서 조회
        3. 메타데이터에서 추론
        4. 기본 타임존 (UTC)
        """
        # 1. 메시지에 명시적 타임존
        if 'timezone' in record:
            return record['timezone']
        
        # 2. 사용자 프로필 캐시
        user_id = record.get('user_id')
        if user_id in self.user_timezone_cache:
            return self.user_timezone_cache[user_id]
        
        # 3. 메타데이터에서 추론
        inferred_tz = self._infer_from_metadata(record)
        if inferred_tz:
            # 캐시에 저장
            self.user_timezone_cache[user_id] = inferred_tz
            return inferred_tz
        
        # 4. 기본값
        return self.default_timezone
    
    def _infer_from_metadata(self, record: Dict[str, Any]) -> Optional[str]:
        """메타데이터에서 타임존 추론"""
        metadata = record.get('metadata', {})
        
        # 디바이스 타임존 설정 (iOS/Android)
        if 'device_timezone' in metadata:
            return metadata['device_timezone']
        
        # 로케일 기반 추론
        if 'locale' in metadata:
            locale = metadata['locale']
            # 예: "ko_KR" → "Asia/Seoul"
            timezone_map = {
                'ko_KR': 'Asia/Seoul',
                'en_US': 'America/New_York',
                'ja_JP': 'Asia/Tokyo',
                'zh_CN': 'Asia/Shanghai',
            }
            return timezone_map.get(locale)
        
        return None
```

**사용 예시:**

```python
from flink_consumer.aggregations.calendar_aggregator import CalendarDailyAggregator

# 타임존 리졸버 초기화
timezone_resolver = HybridTimezoneResolver(default_timezone="UTC")

# 레코드 처리 시 타임존 결정
def process_with_timezone(record):
    user_timezone = timezone_resolver.resolve_timezone(record)
    
    # 타임존 정보를 레코드에 추가
    record['resolved_timezone'] = user_timezone
    
    return record

# 스트림 처리
enriched_stream = data_stream.map(process_with_timezone)

# 사용자별로 다른 타임존 적용
# (현재는 단일 타임존만 지원하므로, 사용자별 분기 필요)
```

### Phase 2: 중기 (메시지 스키마 업데이트)

**Payload 레벨에 타임존 필드 추가**

```json
{
  "deviceId": "iPhone14-ABC123",
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-11-12T10:30:00Z",
  "timezone": "Asia/Seoul",  // ✅ 추가
  "appVersion": "1.2.3",
  "samples": [...]
}
```

**Avro 스키마 업데이트:**

```json
{
  "type": "record",
  "name": "HealthDataPayload",
  "fields": [
    {"name": "deviceId", "type": "string"},
    {"name": "userId", "type": "string"},
    {"name": "timestamp", "type": "string"},
    {"name": "timezone", "type": ["null", "string"], "default": null},  // ✅ 추가
    {"name": "appVersion", "type": "string"},
    {"name": "samples", "type": {"type": "array", "items": "Sample"}}
  ]
}
```

**Flink 처리:**

```python
class HealthDataTransformer(FlatMapFunction):
    def flat_map(self, payload: dict):
        device_id = payload['deviceId']
        user_id = payload['userId']
        user_timezone = payload.get('timezone', 'UTC')  # ✅ 타임존 추출
        
        for sample in payload['samples']:
            row = {
                'device_id': device_id,
                'user_id': user_id,
                'user_timezone': user_timezone,  # ✅ 추가
                'sample_id': sample['id'],
                'data_type': sample['type'],
                'value': sample['value'],
                ...
            }
            yield row

# 집계 시 사용자 타임존 활용
daily_agg = keyed_stream.process(
    CalendarDailyAggregator(
        user_timezone=lambda record: record.get('user_timezone', 'UTC')
    )
)
```

### Phase 3: 장기 (동적 타임존 지원)

**사용자별 동적 타임존 적용**

```python
class DynamicTimezoneAggregator(ProcessFunction):
    """
    레코드별로 다른 타임존을 적용하는 집계
    """
    
    def __init__(self):
        self.state = None
    
    def open(self, runtime_context):
        # 상태 초기화
        state_descriptor = ValueStateDescriptor(
            "timezone_aggregate_state",
            type_info=None
        )
        self.state = runtime_context.get_state(state_descriptor)
    
    def process_element(self, record, ctx):
        # 레코드의 타임존 사용
        user_timezone = record.get('user_timezone', 'UTC')
        
        # 타임존 기준으로 날짜 계산
        from zoneinfo import ZoneInfo
        from datetime import datetime
        
        dt = datetime.fromtimestamp(
            record['start_date'] / 1000,
            tz=ZoneInfo(user_timezone)
        )
        aggregation_date = dt.date()
        
        # 집계 로직...
        # (user_id, data_type, aggregation_date, timezone) 별로 상태 관리
```

## 클라이언트 구현 가이드

### iOS (Swift)

```swift
import Foundation

struct HealthDataPayload: Codable {
    let deviceId: String
    let userId: String
    let timestamp: String
    let timezone: String  // ✅ 추가
    let appVersion: String
    let samples: [HealthSample]
}

// 디바이스 타임존 가져오기
let timezone = TimeZone.current.identifier  // "Asia/Seoul"

let payload = HealthDataPayload(
    deviceId: deviceId,
    userId: userId,
    timestamp: ISO8601DateFormatter().string(from: Date()),
    timezone: timezone,  // ✅ 타임존 포함
    appVersion: appVersion,
    samples: samples
)
```

### Android (Kotlin)

```kotlin
import java.time.ZoneId
import java.time.ZonedDateTime

data class HealthDataPayload(
    val deviceId: String,
    val userId: String,
    val timestamp: String,
    val timezone: String,  // ✅ 추가
    val appVersion: String,
    val samples: List<HealthSample>
)

// 디바이스 타임존 가져오기
val timezone = ZoneId.systemDefault().id  // "Asia/Seoul"

val payload = HealthDataPayload(
    deviceId = deviceId,
    userId = userId,
    timestamp = ZonedDateTime.now().toString(),
    timezone = timezone,  // ✅ 타임존 포함
    appVersion = appVersion,
    samples = samples
)
```

## Iceberg 스키마 업데이트

```sql
-- health_data_raw 테이블에 타임존 필드 추가
ALTER TABLE health_catalog.health_db.health_data_raw
ADD COLUMN user_timezone STRING COMMENT 'User timezone (IANA format)';

-- 집계 테이블에도 추가
ALTER TABLE health_catalog.health_db.health_data_daily_agg
ADD COLUMN user_timezone STRING COMMENT 'Timezone used for aggregation';
```

## 마이그레이션 계획

### 1단계: 하위 호환성 유지

```python
# 타임존 필드가 없는 기존 메시지도 처리
user_timezone = payload.get('timezone', 'UTC')  # 기본값 UTC
```

### 2단계: 점진적 롤아웃

```
Week 1-2: 클라이언트 업데이트 (타임존 필드 추가)
Week 3-4: 모니터링 (타임존 필드 수신율 확인)
Week 5-6: Flink 파이프라인 업데이트 (타임존 활용)
Week 7+: 기본값 제거 (타임존 필수 필드로 전환)
```

### 3단계: 검증

```sql
-- 타임존 분포 확인
SELECT 
    user_timezone,
    COUNT(*) as record_count,
    COUNT(DISTINCT user_id) as user_count
FROM health_data_raw
WHERE ingestion_date >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY user_timezone
ORDER BY record_count DESC;

-- 타임존 누락 확인
SELECT COUNT(*) as missing_timezone_count
FROM health_data_raw
WHERE user_timezone IS NULL
  AND ingestion_date >= CURRENT_DATE - INTERVAL 1 DAY;
```

## 권장사항

1. **단기 (즉시)**: HybridTimezoneResolver 구현
   - 메시지 수정 없이 기존 메타데이터 활용
   - 사용자 프로필 캐시 구축

2. **중기 (1-2개월)**: 메시지 스키마에 timezone 필드 추가
   - 클라이언트 업데이트
   - Avro 스키마 진화
   - 하위 호환성 유지

3. **장기 (3-6개월)**: 동적 타임존 집계 구현
   - 사용자별 타임존 적용
   - 여행 케이스 지원
   - 타임존 변경 이력 추적

## 참고 자료

- [IANA Time Zone Database](https://www.iana.org/time-zones)
- [ISO 8601 Date and Time Format](https://en.wikipedia.org/wiki/ISO_8601)
- [Python zoneinfo](https://docs.python.org/3/library/zoneinfo.html)
- [Avro Schema Evolution](https://avro.apache.org/docs/current/spec.html#Schema+Resolution)
