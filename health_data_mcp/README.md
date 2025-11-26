# Health Data MCP Server

MCP (Model Context Protocol) 서버로, Iceberg 테이블에 저장된 헬스 데이터 집계를 조회할 수 있는 도구를 제공합니다.

## 기능

이 MCP 서버는 다음 도구를 제공합니다:

- `get_daily_aggregates`: 일간 집계 데이터 조회
- `get_weekly_aggregates`: 주간 집계 데이터 조회
- `get_monthly_aggregates`: 월간 집계 데이터 조회
- `get_top_records`: 특정 메트릭의 최고/최저 기록 조회

## 요구사항

- Python 3.11 이상
- Iceberg REST 카탈로그 접근 권한
- S3 호환 스토리지 접근 권한

## 설치

### uv 사용 (권장)

```bash
# uv 설치 (아직 설치하지 않은 경우)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 프로젝트 디렉토리로 이동
cd health_data_mcp

# 의존성 설치
uv sync
```

### pip 사용

```bash
cd health_data_mcp
pip install -e .
```

## 설정

1. `.env.example` 파일을 `.env`로 복사:

```bash
cp .env.example .env
```

2. `.env` 파일을 편집하여 환경에 맞게 설정:

```bash
# Iceberg 카탈로그 설정
ICEBERG_CATALOG_NAME=health_catalog
ICEBERG_CATALOG_URI=http://your-iceberg-rest-catalog:8181
ICEBERG_WAREHOUSE=s3://your-warehouse
ICEBERG_DATABASE=health_data

# S3 설정
S3_ENDPOINT=http://your-s3-endpoint:9000
S3_ACCESS_KEY=your-access-key
S3_SECRET_KEY=your-secret-key
```

## 실행

### 로컬 개발 모드

```bash
# uv 사용
uv run python -m health_data_mcp.main

# 또는 직접 실행
python -m health_data_mcp.main
```

### uvx를 통한 실행

```bash
uvx health-data-mcp
```

## MCP 클라이언트 설정

### Kiro에서 사용

Kiro의 MCP 설정 파일 (`.kiro/settings/mcp.json`)에 다음을 추가:

```json
{
  "mcpServers": {
    "health-data": {
      "command": "uvx",
      "args": ["health-data-mcp"],
      "env": {
        "ICEBERG_CATALOG_URI": "http://localhost:8181",
        "ICEBERG_CATALOG_NAME": "health_catalog",
        "ICEBERG_WAREHOUSE": "s3://warehouse",
        "ICEBERG_DATABASE": "health_data",
        "S3_ENDPOINT": "http://localhost:9000",
        "S3_ACCESS_KEY": "minioadmin",
        "S3_SECRET_KEY": "minioadmin"
      },
      "disabled": false
    }
  }
}
```

또는 로컬 개발 버전 사용:

```json
{
  "mcpServers": {
    "health-data": {
      "command": "python",
      "args": ["-m", "health_data_mcp.main"],
      "cwd": "/path/to/health_data_mcp",
      "env": {
        "ICEBERG_CATALOG_URI": "http://localhost:8181",
        "ICEBERG_CATALOG_NAME": "health_catalog",
        "ICEBERG_WAREHOUSE": "s3://warehouse",
        "ICEBERG_DATABASE": "health_data",
        "S3_ENDPOINT": "http://localhost:9000",
        "S3_ACCESS_KEY": "minioadmin",
        "S3_SECRET_KEY": "minioadmin"
      },
      "disabled": false
    }
  }
}
```

## 도구 사용 가이드

### 1. get_daily_aggregates - 일간 집계 조회

특정 사용자의 일간 집계 데이터를 조회합니다.

**파라미터:**
- `user_id` (필수): 사용자 ID
- `data_type` (필수): 데이터 타입 (예: heartRate, steps, distance)
- `start_date` (선택): 시작 날짜 (YYYY-MM-DD 형식, 기본값: 30일 전)
- `end_date` (선택): 종료 날짜 (YYYY-MM-DD 형식, 기본값: 오늘)

**사용 예시:**

```python
# 기본 사용 (최근 30일)
result = await client.call_tool(
    "get_daily_aggregates",
    {
        "user_id": "user-123",
        "data_type": "heartRate"
    }
)

# 특정 기간 지정
result = await client.call_tool(
    "get_daily_aggregates",
    {
        "user_id": "user-123",
        "data_type": "steps",
        "start_date": "2025-11-01",
        "end_date": "2025-11-30"
    }
)
```

**응답 예시:**

```json
{
  "user_id": "user-123",
  "data_type": "heartRate",
  "date_range": {
    "start": "2025-11-01",
    "end": "2025-11-30"
  },
  "records": [
    {
      "aggregation_date": "2025-11-17",
      "window_start": "2025-11-17T00:00:00+09:00",
      "window_end": "2025-11-17T23:59:59+09:00",
      "min_value": 60.0,
      "max_value": 120.0,
      "avg_value": 75.5,
      "sum_value": 7550.0,
      "count": 100,
      "stddev_value": 12.3,
      "first_value": 65.0,
      "last_value": 70.0,
      "record_count": 100,
      "timezone": "Asia/Seoul"
    }
  ],
  "count": 30
}
```

### 2. get_weekly_aggregates - 주간 집계 조회

특정 사용자의 주간 집계 데이터를 조회합니다.

**파라미터:**
- `user_id` (필수): 사용자 ID
- `data_type` (필수): 데이터 타입
- `start_week` (선택): 시작 주 (YYYY-Www 형식, 예: 2025-W40, 기본값: 12주 전)
- `end_week` (선택): 종료 주 (YYYY-Www 형식, 기본값: 이번 주)

**사용 예시:**

```python
# 기본 사용 (최근 12주)
result = await client.call_tool(
    "get_weekly_aggregates",
    {
        "user_id": "user-123",
        "data_type": "steps"
    }
)

# 특정 주 범위 지정
result = await client.call_tool(
    "get_weekly_aggregates",
    {
        "user_id": "user-123",
        "data_type": "steps",
        "start_week": "2025-W40",
        "end_week": "2025-W46"
    }
)
```

**응답 예시:**

```json
{
  "user_id": "user-123",
  "data_type": "steps",
  "week_range": {
    "start_week": "2025-W40",
    "end_week": "2025-W46"
  },
  "records": [
    {
      "year": 2025,
      "week_of_year": 46,
      "week_start_date": "2025-11-10",
      "week_end_date": "2025-11-16",
      "window_start": "2025-11-10T00:00:00+09:00",
      "window_end": "2025-11-16T23:59:59+09:00",
      "min_value": 5000.0,
      "max_value": 15000.0,
      "avg_value": 10500.0,
      "sum_value": 73500.0,
      "count": 7,
      "stddev_value": 2500.0,
      "daily_avg_of_avg": 10500.0,
      "record_count": 700,
      "timezone": "Asia/Seoul"
    }
  ],
  "count": 7
}
```

### 3. get_monthly_aggregates - 월간 집계 조회

특정 사용자의 월간 집계 데이터를 조회합니다.

**파라미터:**
- `user_id` (필수): 사용자 ID
- `data_type` (필수): 데이터 타입
- `start_month` (선택): 시작 월 (YYYY-MM 형식, 기본값: 6개월 전)
- `end_month` (선택): 종료 월 (YYYY-MM 형식, 기본값: 이번 달)

**사용 예시:**

```python
# 기본 사용 (최근 6개월)
result = await client.call_tool(
    "get_monthly_aggregates",
    {
        "user_id": "user-123",
        "data_type": "heartRate"
    }
)

# 특정 월 범위 지정
result = await client.call_tool(
    "get_monthly_aggregates",
    {
        "user_id": "user-123",
        "data_type": "heartRate",
        "start_month": "2025-06",
        "end_month": "2025-11"
    }
)
```

**응답 예시:**

```json
{
  "user_id": "user-123",
  "data_type": "heartRate",
  "month_range": {
    "start_month": "2025-06",
    "end_month": "2025-11"
  },
  "records": [
    {
      "year": 2025,
      "month": 11,
      "month_start_date": "2025-11-01",
      "month_end_date": "2025-11-30",
      "days_in_month": 30,
      "window_start": "2025-11-01T00:00:00+09:00",
      "window_end": "2025-11-30T23:59:59+09:00",
      "min_value": 55.0,
      "max_value": 130.0,
      "avg_value": 74.2,
      "sum_value": 222600.0,
      "count": 3000,
      "stddev_value": 13.1,
      "daily_avg_of_avg": 74.2,
      "record_count": 3000,
      "timezone": "Asia/Seoul"
    }
  ],
  "count": 6
}
```

### 4. get_top_records - 최고/최저 기록 조회

특정 기간 내에서 특정 메트릭의 최고 또는 최저 기록을 조회합니다.

**파라미터:**
- `user_id` (필수): 사용자 ID
- `data_type` (필수): 데이터 타입
- `start_date` (선택): 시작 날짜 (YYYY-MM-DD 형식, 기본값: 30일 전)
- `end_date` (선택): 종료 날짜 (YYYY-MM-DD 형식, 기본값: 오늘)
- `sort_by` (선택): 정렬 기준 (max_value, avg_value, sum_value, count, 기본값: max_value)
- `order` (선택): 정렬 순서 (asc, desc, 기본값: desc)
- `limit` (선택): 결과 개수 (1-10000, 기본값: 10)

**사용 예시:**

```python
# 최근 30일 중 가장 많이 걸었던 날 상위 10개
result = await client.call_tool(
    "get_top_records",
    {
        "user_id": "user-123",
        "data_type": "steps",
        "sort_by": "sum_value",
        "order": "desc",
        "limit": 10
    }
)

# 특정 기간의 평균 심박수가 가장 낮았던 날 상위 5개
result = await client.call_tool(
    "get_top_records",
    {
        "user_id": "user-123",
        "data_type": "heartRate",
        "start_date": "2025-11-01",
        "end_date": "2025-11-30",
        "sort_by": "avg_value",
        "order": "asc",
        "limit": 5
    }
)

# 최고 심박수 기록 조회
result = await client.call_tool(
    "get_top_records",
    {
        "user_id": "user-123",
        "data_type": "heartRate",
        "sort_by": "max_value",
        "order": "desc",
        "limit": 1
    }
)
```

**응답 예시:**

```json
{
  "user_id": "user-123",
  "data_type": "steps",
  "date_range": {
    "start": "2025-11-01",
    "end": "2025-11-30"
  },
  "sort_by": "sum_value",
  "order": "desc",
  "records": [
    {
      "rank": 1,
      "aggregation_date": "2025-11-15",
      "min_value": 0.0,
      "max_value": 18500.0,
      "avg_value": 18500.0,
      "sum_value": 18500.0,
      "count": 1,
      "stddev_value": 0.0
    },
    {
      "rank": 2,
      "aggregation_date": "2025-11-22",
      "min_value": 0.0,
      "max_value": 17200.0,
      "avg_value": 17200.0,
      "sum_value": 17200.0,
      "count": 1,
      "stddev_value": 0.0
    }
  ],
  "count": 10
}
```

## 에러 처리

MCP 서버는 다양한 에러 상황을 명확한 메시지와 함께 처리합니다.

### 에러 응답 형식

모든 에러는 다음 형식으로 반환됩니다:

```json
{
  "error": {
    "type": "ValidationError",
    "message": "Invalid date format. Expected YYYY-MM-DD",
    "details": {
      "field": "start_date",
      "provided": "2025/11/17",
      "expected": "YYYY-MM-DD"
    }
  }
}
```

### 주요 에러 타입

#### 1. ValidationError - 파라미터 검증 오류

잘못된 파라미터가 제공되었을 때 발생합니다.

**예시:**

```python
# 잘못된 날짜 형식
result = await client.call_tool(
    "get_daily_aggregates",
    {
        "user_id": "user-123",
        "data_type": "heartRate",
        "start_date": "2025/11/17"  # 잘못된 형식
    }
)
# 응답: {"error": {"type": "ValidationError", "message": "Invalid date format..."}}

# 필수 파라미터 누락
result = await client.call_tool(
    "get_daily_aggregates",
    {
        "user_id": "user-123"
        # data_type 누락
    }
)
# 응답: {"error": {"type": "ValidationError", "message": "Missing required parameter: data_type"}}

# 날짜 범위 오류
result = await client.call_tool(
    "get_daily_aggregates",
    {
        "user_id": "user-123",
        "data_type": "heartRate",
        "start_date": "2025-11-30",
        "end_date": "2025-11-01"  # end_date가 start_date보다 이전
    }
)
# 응답: {"error": {"type": "ValidationError", "message": "start_date must be before or equal to end_date"}}

# 잘못된 정렬 기준
result = await client.call_tool(
    "get_top_records",
    {
        "user_id": "user-123",
        "data_type": "steps",
        "sort_by": "invalid_field"  # 잘못된 필드
    }
)
# 응답: {"error": {"type": "ValidationError", "message": "Invalid sort_by value..."}}
```

#### 2. NotFoundError - 데이터 없음

요청한 데이터가 존재하지 않을 때 발생합니다.

**예시:**

```python
# 존재하지 않는 테이블
result = await client.call_tool(
    "get_daily_aggregates",
    {
        "user_id": "user-123",
        "data_type": "heartRate"
    }
)
# 응답: {"error": {"type": "NotFoundError", "message": "Table not found: health_data.health_data_daily_agg"}}

# 데이터가 없는 경우 (에러가 아닌 빈 결과 반환)
result = await client.call_tool(
    "get_daily_aggregates",
    {
        "user_id": "non-existent-user",
        "data_type": "heartRate"
    }
)
# 응답: {"user_id": "non-existent-user", "records": [], "count": 0}
```

#### 3. ConnectionError - 연결 오류

Iceberg 카탈로그 또는 S3 연결에 실패했을 때 발생합니다.

**예시:**

```python
# 카탈로그 연결 실패
result = await client.call_tool(
    "get_daily_aggregates",
    {
        "user_id": "user-123",
        "data_type": "heartRate"
    }
)
# 응답: {"error": {"type": "ConnectionError", "message": "Failed to connect to Iceberg catalog..."}}
```

#### 4. InternalError - 내부 오류

예상치 못한 오류가 발생했을 때 반환됩니다.

**예시:**

```python
# 서버 내부 오류
result = await client.call_tool(
    "get_daily_aggregates",
    {
        "user_id": "user-123",
        "data_type": "heartRate"
    }
)
# 응답: {"error": {"type": "InternalError", "message": "An unexpected error occurred"}}
```

### 에러 처리 모범 사례

```python
async def query_health_data(user_id: str, data_type: str):
    """에러 처리를 포함한 헬스 데이터 조회"""
    try:
        result = await client.call_tool(
            "get_daily_aggregates",
            {
                "user_id": user_id,
                "data_type": data_type
            }
        )
        
        # 에러 응답 확인
        if "error" in result:
            error = result["error"]
            error_type = error.get("type")
            
            if error_type == "ValidationError":
                print(f"파라미터 오류: {error['message']}")
                # 파라미터 수정 후 재시도
            elif error_type == "NotFoundError":
                print(f"데이터 없음: {error['message']}")
                # 다른 데이터 소스 시도
            elif error_type == "ConnectionError":
                print(f"연결 오류: {error['message']}")
                # 재연결 시도
            else:
                print(f"알 수 없는 오류: {error['message']}")
            
            return None
        
        # 정상 응답 처리
        if result["count"] == 0:
            print("조회된 데이터가 없습니다.")
            return None
        
        return result["records"]
        
    except Exception as e:
        print(f"클라이언트 오류: {e}")
        return None
```

## 성능 최적화 팁

### 1. 날짜 범위 최적화

불필요하게 넓은 날짜 범위는 성능을 저하시킵니다. 필요한 범위만 조회하세요.

```python
# 나쁜 예: 1년 전체 조회
result = await client.call_tool(
    "get_daily_aggregates",
    {
        "user_id": "user-123",
        "data_type": "heartRate",
        "start_date": "2024-01-01",
        "end_date": "2024-12-31"
    }
)

# 좋은 예: 필요한 기간만 조회
result = await client.call_tool(
    "get_daily_aggregates",
    {
        "user_id": "user-123",
        "data_type": "heartRate",
        "start_date": "2025-11-01",
        "end_date": "2025-11-30"
    }
)
```

### 2. 적절한 집계 레벨 선택

분석 목적에 맞는 집계 레벨을 선택하세요.

```python
# 장기 트렌드 분석: 월간 집계 사용
result = await client.call_tool(
    "get_monthly_aggregates",
    {
        "user_id": "user-123",
        "data_type": "heartRate",
        "start_month": "2025-01",
        "end_month": "2025-11"
    }
)

# 주간 패턴 분석: 주간 집계 사용
result = await client.call_tool(
    "get_weekly_aggregates",
    {
        "user_id": "user-123",
        "data_type": "steps"
    }
)

# 상세 분석: 일간 집계 사용 (짧은 기간)
result = await client.call_tool(
    "get_daily_aggregates",
    {
        "user_id": "user-123",
        "data_type": "heartRate",
        "start_date": "2025-11-20",
        "end_date": "2025-11-26"
    }
)
```

### 3. Top Records 제한 설정

필요한 개수만큼만 조회하여 성능을 향상시키세요.

```python
# 나쁜 예: 과도한 레코드 조회
result = await client.call_tool(
    "get_top_records",
    {
        "user_id": "user-123",
        "data_type": "steps",
        "limit": 1000  # 너무 많음
    }
)

# 좋은 예: 필요한 개수만 조회
result = await client.call_tool(
    "get_top_records",
    {
        "user_id": "user-123",
        "data_type": "steps",
        "limit": 10  # 상위 10개만
    }
)
```

### 4. 병렬 쿼리 활용

여러 데이터 타입을 조회할 때는 병렬로 실행하세요.

```python
import asyncio

# 나쁜 예: 순차 실행
heart_rate = await client.call_tool("get_daily_aggregates", {...})
steps = await client.call_tool("get_daily_aggregates", {...})
distance = await client.call_tool("get_daily_aggregates", {...})

# 좋은 예: 병렬 실행
results = await asyncio.gather(
    client.call_tool("get_daily_aggregates", {"user_id": "user-123", "data_type": "heartRate"}),
    client.call_tool("get_daily_aggregates", {"user_id": "user-123", "data_type": "steps"}),
    client.call_tool("get_daily_aggregates", {"user_id": "user-123", "data_type": "distance"})
)
heart_rate, steps, distance = results
```

### 5. 캐싱 전략

자주 조회하는 데이터는 클라이언트 측에서 캐싱하세요.

```python
from functools import lru_cache
from datetime import datetime, timedelta

class HealthDataCache:
    def __init__(self, client):
        self.client = client
        self.cache = {}
        self.cache_ttl = timedelta(minutes=5)
    
    async def get_daily_aggregates(self, user_id: str, data_type: str, start_date: str, end_date: str):
        cache_key = f"{user_id}:{data_type}:{start_date}:{end_date}"
        
        # 캐시 확인
        if cache_key in self.cache:
            cached_data, cached_time = self.cache[cache_key]
            if datetime.now() - cached_time < self.cache_ttl:
                return cached_data
        
        # 캐시 미스: 서버에서 조회
        result = await self.client.call_tool(
            "get_daily_aggregates",
            {
                "user_id": user_id,
                "data_type": data_type,
                "start_date": start_date,
                "end_date": end_date
            }
        )
        
        # 캐시 저장
        self.cache[cache_key] = (result, datetime.now())
        return result
```

### 6. Iceberg 파티션 활용

Iceberg 테이블은 `user_id`, `aggregation_date`, `data_type`로 파티션되어 있습니다. 이러한 필드를 필터로 사용하면 파티션 프루닝이 적용되어 성능이 크게 향상됩니다.

```python
# 최적화됨: 파티션 필드 모두 사용
result = await client.call_tool(
    "get_daily_aggregates",
    {
        "user_id": "user-123",        # 파티션 필드
        "data_type": "heartRate",     # 파티션 필드
        "start_date": "2025-11-01",   # 파티션 필드
        "end_date": "2025-11-30"      # 파티션 필드
    }
)
```

### 7. 연결 재사용

MCP 서버는 Iceberg 카탈로그 연결을 재사용합니다. 서버를 재시작하지 않고 계속 사용하세요.

### 8. 로그 레벨 조정

프로덕션 환경에서는 로그 레벨을 INFO 이상으로 설정하여 불필요한 로깅 오버헤드를 줄이세요.

```bash
# 개발 환경
LOG_LEVEL=DEBUG

# 프로덕션 환경
LOG_LEVEL=INFO
```

### 성능 벤치마크

일반적인 쿼리 성능 (로컬 환경, MinIO + Iceberg REST):

- 일간 집계 조회 (30일): ~200-500ms
- 주간 집계 조회 (12주): ~150-300ms
- 월간 집계 조회 (6개월): ~100-200ms
- Top Records 조회 (10개): ~200-400ms

성능은 다음 요인에 영향을 받습니다:
- 네트워크 지연 (Iceberg 카탈로그 및 S3)
- 데이터 크기 및 파티션 수
- 필터 조건의 선택도
- 서버 리소스 (CPU, 메모리)

## 개발

### 테스트 실행

```bash
# 모든 테스트 실행
uv run pytest

# 특정 테스트 파일 실행
uv run pytest tests/test_config.py

# 커버리지와 함께 실행
uv run pytest --cov=health_data_mcp
```

### 코드 포맷팅 및 린팅

```bash
# Ruff로 린팅
uv run ruff check .

# 자동 수정
uv run ruff check --fix .

# 포맷팅
uv run ruff format .
```

## 프로젝트 구조

```
health_data_mcp/
├── pyproject.toml          # 프로젝트 메타데이터 및 의존성
├── .env.example            # 환경 변수 예시
├── README.md               # 이 파일
├── health_data_mcp/
│   ├── __init__.py
│   ├── main.py             # MCP 서버 진입점
│   ├── config.py           # 설정 관리
│   ├── services/
│   │   ├── __init__.py
│   │   ├── query_service.py    # 쿼리 서비스
│   │   └── iceberg_client.py   # Iceberg 클라이언트
│   └── models/
│       ├── __init__.py
│       └── responses.py    # 응답 모델
└── tests/
    ├── __init__.py
    ├── test_query_service.py
    ├── test_iceberg_client.py
    └── test_integration.py
```

## 로깅 설정

MCP 서버는 구조화된 로깅을 지원하며, 환경 변수를 통해 설정할 수 있습니다.

### 로그 레벨

`.env` 파일에서 로그 레벨을 설정:

```bash
# DEBUG, INFO, WARNING, ERROR, CRITICAL 중 선택
LOG_LEVEL=INFO
```

개발 환경(`ENVIRONMENT=development`)에서는 자동으로 DEBUG 레벨이 활성화됩니다.

### 로그 형식

두 가지 로그 형식을 지원합니다:

1. **human** (기본값): 사람이 읽기 쉬운 형식, 개발 환경에 적합
   ```bash
   LOG_FORMAT=human
   ```
   
   출력 예시:
   ```
   2025-11-26 10:30:45 - health_data_mcp.main - INFO - Tool called: get_daily_aggregates
   ```

2. **json**: 구조화된 JSON 로그, 프로덕션 환경 및 로그 분석 도구에 적합
   ```bash
   LOG_FORMAT=json
   ```
   
   출력 예시:
   ```json
   {
     "timestamp": "2025-11-26T10:30:45.123Z",
     "level": "INFO",
     "logger": "health_data_mcp.main",
     "message": "Tool called: get_daily_aggregates",
     "tool_name": "get_daily_aggregates",
     "params": {"user_id": "user-123", "data_type": "heartRate"}
   }
   ```

### 로그 파일

로그를 파일에 저장하려면:

```bash
LOG_FILE=/var/log/health-data-mcp/server.log
```

파일 로그는 항상 JSON 형식으로 저장됩니다.

### 민감한 정보 마스킹

로그에서 다음과 같은 민감한 정보가 자동으로 마스킹됩니다:

- 비밀번호 (password, passwd, pwd)
- 시크릿 키 (secret, secret_key)
- 토큰 (token, access_token)
- API 키 (api_key, apikey)
- S3 자격 증명 (s3_secret_key)

마스킹 예시:
```
s3_secret_key=***MASKED***
```

### 환경별 설정

**개발 환경:**
```bash
ENVIRONMENT=development
LOG_LEVEL=DEBUG
LOG_FORMAT=human
```

**프로덕션 환경:**
```bash
ENVIRONMENT=production
LOG_LEVEL=INFO
LOG_FORMAT=json
LOG_FILE=/var/log/health-data-mcp/server.log
```

## 문제 해결

### Iceberg 카탈로그 연결 실패

- `ICEBERG_CATALOG_URI`가 올바른지 확인
- 네트워크 연결 확인
- 카탈로그 서버가 실행 중인지 확인
- 로그에서 상세한 오류 메시지 확인 (`LOG_LEVEL=DEBUG` 설정)

### S3 접근 오류

- S3 엔드포인트, 액세스 키, 시크릿 키 확인
- S3 버킷 권한 확인
- 로그에서 연결 오류 확인

### 테이블을 찾을 수 없음

- `ICEBERG_DATABASE` 설정 확인
- 테이블 이름이 올바른지 확인
- 카탈로그에 테이블이 존재하는지 확인
- DEBUG 로그에서 사용 가능한 테이블 목록 확인

### 로그 확인

서버 시작 시 다음 정보가 로그에 출력됩니다:
- 카탈로그 URI 및 데이터베이스
- 사용 가능한 테이블 목록
- 기본 쿼리 범위 설정
- 등록된 도구 목록

각 도구 호출 시:
- 호출된 도구 이름 및 파라미터
- 실행 시간 및 결과 레코드 수
- 오류 발생 시 스택 트레이스

## 라이선스

MIT License

## 기여

이슈와 풀 리퀘스트를 환영합니다!
