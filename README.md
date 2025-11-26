# Health Stack Data Platform

실시간 헬스 데이터 수집, 처리, 분석을 위한 엔드-투-엔드 데이터 플랫폼입니다. iOS 앱에서 수집된 헬스 데이터를 Kafka를 통해 수집하고, Apache Flink로 실시간 처리하여 Apache Iceberg 데이터 레이크에 저장하며, MCP 서버를 통해 AI 에이전트가 데이터를 조회할 수 있습니다.

## 시스템 아키텍처

```
┌─────────────┐
│  iOS App    │
└──────┬──────┘
       │ JSON/HTTPS
       ▼
┌──────────────────────────────────────────┐
│      API Gateway (FastAPI)               │
│  ┌──────────┐  ┌────────────────┐       │
│  │Validator │→ │Avro Converter  │       │
│  └──────────┘  └────────┬───────┘       │
└─────────────────────────┼────────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │  Schema Registry      │
              └───────────────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │   Kafka Cluster       │
              │  (3 brokers)          │
              │  health-data-raw      │
              └───────────┬───────────┘
                          │
                          ▼
┌──────────────────────────────────────────┐
│      Apache Flink Cluster                │
│  ┌────────────────────────────────────┐  │
│  │  Stream Processing Pipeline        │  │
│  │  - Transformation                  │  │
│  │  - Validation                      │  │
│  │  - Time-based Aggregation          │  │
│  │    (Daily/Weekly/Monthly)          │  │
│  └────────────┬───────────────────────┘  │
└───────────────┼──────────────────────────┘
                │
                ▼
┌──────────────────────────────────────────┐
│      Apache Iceberg Data Lake            │
│  ┌──────────────────────────────────┐   │
│  │ health_data_raw                  │   │
│  │ health_data_daily_agg            │   │
│  │ health_data_weekly_agg           │   │
│  │ health_data_monthly_agg          │   │
│  └──────────────────────────────────┘   │
│                                          │
│  Storage: MinIO (S3-compatible)         │
└──────────────┬───────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────┐
│      Health Data MCP Server              │
│  ┌──────────────────────────────────┐   │
│  │  Query Tools for AI Agents       │   │
│  │  - get_daily_aggregates          │   │
│  │  - get_weekly_aggregates         │   │
│  │  - get_monthly_aggregates        │   │
│  │  - get_top_records               │   │
│  └──────────────────────────────────┘   │
└──────────────┬───────────────────────────┘
               │
               ▼
         ┌──────────┐
         │ AI Agent │
         │  (Kiro)  │
         └──────────┘
```

## 주요 컴포넌트

### 1. API Gateway (FastAPI)
iOS 앱에서 전송된 헬스 데이터를 수신하고 Kafka로 전달하는 REST API 게이트웨이입니다.

**주요 기능:**
- JSON 데이터 검증 및 변환
- Avro 포맷으로 직렬화
- Schema Registry 연동
- Kafka 메시지 발행
- 에러 처리 및 DLQ (Dead Letter Queue)

**기술 스택:**
- FastAPI (Python 3.11+)
- Confluent Kafka Python
- Avro Schema Registry
- Pydantic 데이터 검증

**포트:**
- API: http://localhost:3000
- Swagger UI: http://localhost:3000/docs
- Health Check: http://localhost:3000/health

📖 **상세 문서:** [gateway/README.md](gateway/README.md)

---

### 2. Kafka Cluster
고가용성 메시지 브로커로 헬스 데이터 스트림을 안정적으로 전달합니다.

**구성:**
- 3개의 Kafka 브로커 (KRaft 모드)
- Schema Registry (Avro 스키마 관리)
- Kafka UI (모니터링 및 관리)

**토픽:**
- `health-data-raw`: 원시 헬스 데이터 (6 파티션, RF=3)
- `health-data-dlq`: 실패한 메시지 (3 파티션, RF=3)

**포트:**
- Broker 1: localhost:19092
- Broker 2: localhost:19093
- Broker 3: localhost:19094
- Schema Registry: http://localhost:8081
- Kafka UI: http://localhost:8080

---

### 3. Flink Consumer (Apache Flink)
실시간 스트림 처리 애플리케이션으로 Kafka에서 데이터를 소비하고 Iceberg에 저장합니다.

**주요 기능:**
- 실시간 데이터 변환 및 검증
- 시간 기반 집계 (일간/주간/월간)
- Exactly-once 시맨틱 보장
- 늦게 도착한 데이터 처리 (Late Data Handling)
- 체크포인트 기반 장애 복구

**집계 통계:**
- min_value, max_value, avg_value
- sum_value, count, stddev_value
- first_value, last_value

**기술 스택:**
- Apache Flink 1.18+
- PyFlink (Python API)
- Apache Iceberg
- MinIO (S3-compatible storage)

**포트:**
- Flink Web UI: http://localhost:8081
- Prometheus Metrics: http://localhost:9249

📖 **상세 문서:** [flink_consumer/README.md](flink_consumer/README.md)

---

### 4. Apache Iceberg Data Lake
확장 가능한 데이터 레이크로 헬스 데이터를 효율적으로 저장하고 쿼리합니다.

**테이블:**
- `health_data_raw`: 원시 헬스 데이터
- `health_data_daily_agg`: 일간 집계
- `health_data_weekly_agg`: 주간 집계
- `health_data_monthly_agg`: 월간 집계
- `health_data_errors`: 에러 로그 (DLQ)

**파티셔닝:**
- `user_id` (해시 파티션)
- `aggregation_date` (날짜 파티션)
- `data_type` (카테고리 파티션)

**스토리지:**
- MinIO (S3-compatible)
- Warehouse: s3a://data-lake/warehouse
- Checkpoints: s3a://flink-checkpoints

**포트:**
- MinIO API: http://localhost:9000
- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)

---

### 5. Health Data MCP Server
AI 에이전트가 Iceberg 데이터 레이크의 헬스 데이터를 조회할 수 있는 MCP (Model Context Protocol) 서버입니다.

**제공 도구:**
- `get_daily_aggregates`: 일간 집계 조회
- `get_weekly_aggregates`: 주간 집계 조회
- `get_monthly_aggregates`: 월간 집계 조회
- `get_top_records`: 최고/최저 기록 조회

**기술 스택:**
- MCP SDK (Python)
- PyIceberg
- PyArrow

**사용 예시:**
```python
# Kiro AI 에이전트에서 사용
"최근 30일간 user-123의 심박수 평균을 알려줘"
→ get_daily_aggregates(user_id="user-123", data_type="heartRate")

"이번 달 가장 많이 걸었던 날은?"
→ get_top_records(user_id="user-123", data_type="steps", sort_by="sum_value")
```

📖 **상세 문서:** [health_data_mcp/README.md](health_data_mcp/README.md)

---

## 빠른 시작

### 사전 요구사항

- Docker & Docker Compose
- Python 3.11+ (로컬 개발 시)
- uv (Python 패키지 매니저)

```bash
# uv 설치
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 전체 스택 실행

```bash
# 1. 모든 서비스 시작
docker-compose up -d

# 2. 서비스 상태 확인
docker-compose ps

# 3. 로그 확인
docker-compose logs -f

# 4. 서비스 접속
# - API Gateway: http://localhost:3000/docs
# - Kafka UI: http://localhost:8080
# - Flink UI: http://localhost:8081
# - MinIO Console: http://localhost:9001
```

### 데이터 전송 테스트

```bash
# 샘플 헬스 데이터 전송
curl -X POST http://localhost:3000/api/v1/health-data \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user-123",
    "timestamp": "2025-11-26T10:30:00Z",
    "dataType": "heart_rate",
    "value": 72,
    "unit": "bpm",
    "metadata": {
      "deviceId": "iPhone14-ABC123",
      "appVersion": "1.2.3",
      "platform": "iOS"
    }
  }'
```

### 데이터 확인

```bash
# 1. Kafka UI에서 메시지 확인
# http://localhost:8080 → Topics → health-data-raw

# 2. Flink UI에서 처리 상태 확인
# http://localhost:8081 → Jobs

# 3. MinIO에서 Iceberg 파일 확인
# http://localhost:9001 → data-lake → warehouse
```

---

## MCP 서버 설정 (Kiro)

### 1. MCP 서버 설치

```bash
cd health_data_mcp
uv sync
```

### 2. Kiro 설정 파일 추가

`.kiro/settings/mcp.json` 파일에 다음 추가:

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
        "ICEBERG_WAREHOUSE": "s3://data-lake/warehouse",
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

### 3. Kiro에서 사용

```
"user-123의 최근 30일 심박수 데이터를 분석해줘"
"이번 주 가장 많이 걸었던 날은?"
"지난 6개월 월별 평균 심박수 추이를 보여줘"
```

---

## 데이터 흐름

### 1. 데이터 수집 (Ingestion)
```
iOS App → API Gateway → Kafka (health-data-raw)
```

### 2. 실시간 처리 (Processing)
```
Kafka → Flink Consumer → Transformation → Validation
```

### 3. 집계 (Aggregation)
```
Flink → Time Windows (Daily/Weekly/Monthly) → Statistics
```

### 4. 저장 (Storage)
```
Flink → Iceberg Tables → MinIO (S3)
```

### 5. 조회 (Query)
```
AI Agent → MCP Server → PyIceberg → Iceberg Tables
```

---

## 지원하는 데이터 타입

| 데이터 타입 | 설명 | 단위 | 예시 값 |
|------------|------|------|---------|
| `heart_rate` | 심박수 | bpm | 72 |
| `steps` | 걸음 수 | count | 8543 |
| `distance` | 이동 거리 | km | 5.2 |
| `blood_pressure` | 혈압 | mmHg | {systolic: 120, diastolic: 80} |
| `blood_glucose` | 혈당 | mg/dL | 95 |
| `body_temperature` | 체온 | °C | 36.8 |
| `oxygen_saturation` | 산소포화도 | % | 98 |
| `respiratory_rate` | 호흡수 | breaths/min | 16 |
| `weight` | 체중 | kg | 70.5 |
| `sleep` | 수면 | minutes | {duration: 480, ...} |

---

## 모니터링 및 관리

### 서비스 상태 확인

```bash
# 모든 서비스 상태
docker-compose ps

# 특정 서비스 로그
docker-compose logs -f gateway
docker-compose logs -f flink-jobmanager
docker-compose logs -f kafka-broker-1
```

### 웹 UI 접속

| 서비스 | URL | 설명 |
|--------|-----|------|
| API Gateway | http://localhost:3000/docs | Swagger UI |
| Kafka UI | http://localhost:8080 | Kafka 토픽 및 메시지 |
| Flink UI | http://localhost:8081 | Flink 작업 모니터링 |
| MinIO Console | http://localhost:9001 | S3 스토리지 관리 |

### 헬스 체크

```bash
# API Gateway
curl http://localhost:3000/health

# Flink JobManager
curl http://localhost:8081/overview

# Schema Registry
curl http://localhost:8081/subjects
```

### 메트릭 수집

```bash
# API Gateway 메트릭 (Prometheus)
curl http://localhost:3000/metrics

# Flink 메트릭
curl http://localhost:9249/metrics
```

---

## 개발 환경 설정

### Gateway 로컬 개발

```bash
cd gateway
uv sync
cp .env.example .env
# .env 파일 수정
uv run uvicorn main:app --reload --host 0.0.0.0 --port 3000
```

### Flink Consumer 로컬 개발

```bash
cd flink_consumer
uv sync
cp .env.example .env.local
# .env.local 파일 수정
source .venv/bin/activate
python main.py
```

### MCP Server 로컬 개발

```bash
cd health_data_mcp
uv sync
cp .env.example .env
# .env 파일 수정
python -m health_data_mcp.main
```

---

## 테스트

### Gateway 테스트

```bash
cd gateway
uv run pytest
```

### Flink Consumer 테스트

```bash
cd flink_consumer
uv run pytest
```

### MCP Server 테스트

```bash
cd health_data_mcp
uv run pytest
```

### 통합 테스트

```bash
# 전체 스택 통합 테스트
./run_integration_tests.sh
```

---

## 성능 및 확장성

### 처리량 (Throughput)
- **API Gateway**: ~10,000 req/sec (단일 인스턴스)
- **Kafka**: ~100,000 msg/sec (3 브로커)
- **Flink**: ~50,000 records/sec (12 태스크 슬롯)

### 확장 방법

**Gateway 수평 확장:**
```bash
docker-compose up -d --scale gateway=3
```

**Flink TaskManager 확장:**
```bash
docker-compose up -d --scale flink-taskmanager=5
```

**Kafka 파티션 증가:**
```bash
docker-compose exec kafka-broker-1 kafka-topics \
  --alter --topic health-data-raw \
  --partitions 12 \
  --bootstrap-server kafka-broker-1:9092
```

---

## 장애 복구

### Flink 체크포인트
- 60초 간격으로 자동 체크포인트
- S3 (MinIO)에 상태 저장
- 장애 발생 시 자동 복구

### Kafka 복제
- 3개 브로커에 데이터 복제 (RF=3)
- 최소 2개 브로커 동기화 (min.insync.replicas=2)
- 브로커 장애 시 자동 페일오버

### 데이터 무결성
- Exactly-once 시맨틱 보장
- 트랜잭션 기반 Kafka 프로듀서
- Flink 체크포인트 기반 상태 관리

---

## 문제 해결

### 서비스가 시작되지 않을 때

```bash
# 로그 확인
docker-compose logs <service-name>

# 서비스 재시작
docker-compose restart <service-name>

# 전체 재시작
docker-compose down
docker-compose up -d
```

### Kafka 연결 오류

```bash
# Kafka 브로커 상태 확인
docker-compose exec kafka-broker-1 kafka-broker-api-versions \
  --bootstrap-server kafka-broker-1:9092

# 토픽 목록 확인
docker-compose exec kafka-broker-1 kafka-topics \
  --list --bootstrap-server kafka-broker-1:9092
```

### Flink 작업 실패

```bash
# Flink 로그 확인
docker-compose logs flink-jobmanager
docker-compose logs flink-taskmanager-1

# Flink UI에서 작업 상태 확인
# http://localhost:8081
```

### 상세 문제 해결 가이드
- [Gateway 문제 해결](docs/TROUBLESHOOTING.md)
- [Flink 문제 해결](flink_consumer/docs/DEPLOYMENT.md)

---

## 프로덕션 배포

### 환경 변수 설정

```bash
# 프로덕션 환경 변수 설정
LOG_LEVEL=INFO
ENVIRONMENT=production
```

### 보안 설정

```bash
# API Gateway HTTPS 활성화
SSL_ENABLED=true
SSL_CERTFILE=/path/to/cert.pem
SSL_KEYFILE=/path/to/key.pem

# Kafka SASL 인증 (선택사항)
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
```

### Kubernetes 배포

```bash
# Flink Operator 설치
kubectl apply -f flink_consumer/k8s/

# Gateway 배포
kubectl apply -f gateway/k8s/
```

---

## 문서

### 컴포넌트별 상세 문서
- **[API Gateway](gateway/README.md)** - FastAPI 게이트웨이 가이드
- **[Flink Consumer](flink_consumer/README.md)** - 스트림 처리 파이프라인
- **[MCP Server](health_data_mcp/README.md)** - AI 에이전트 통합

### API 문서
- **[API Reference](docs/API.md)** - REST API 명세
- **[OpenAPI Spec](docs/openapi.yaml)** - OpenAPI 3.0 스펙
- **[Examples](docs/EXAMPLES.md)** - 사용 예시

### 운영 가이드
- **[Environment Variables](docs/ENVIRONMENT_VARIABLES.md)** - 환경 변수 설정
- **[Troubleshooting](docs/TROUBLESHOOTING.md)** - 문제 해결
- **[Deployment](flink_consumer/docs/DEPLOYMENT.md)** - 배포 가이드

### 아키텍처 문서
- **[Aggregation Pipeline](flink_consumer/docs/AGGREGATION_PIPELINE.md)** - 집계 파이프라인
- **[Iceberg Setup](flink_consumer/docs/ICEBERG_SETUP.md)** - Iceberg 설정
- **[Schema Evolution](flink_consumer/docs/SCHEMA_EVOLUTION.md)** - 스키마 진화

---

## 기술 스택

### 백엔드
- **Python 3.11+** - 주 프로그래밍 언어
- **FastAPI** - API Gateway 프레임워크
- **Apache Flink 1.18+** - 스트림 처리
- **Apache Kafka 7.5** - 메시지 브로커
- **Apache Iceberg** - 데이터 레이크 테이블 포맷

### 스토리지
- **MinIO** - S3-compatible 오브젝트 스토리지
- **Confluent Schema Registry** - Avro 스키마 관리

### 모니터링
- **Prometheus** - 메트릭 수집
- **Grafana** - 메트릭 시각화 (선택사항)
- **Kafka UI** - Kafka 모니터링

### 개발 도구
- **uv** - Python 패키지 매니저
- **Docker & Docker Compose** - 컨테이너화
- **pytest** - 테스트 프레임워크
- **ruff** - 린터 및 포매터

---

## 라이선스

Proprietary - Health Stack Project

---

## 기여

이슈와 풀 리퀘스트를 환영합니다!

---

## 지원

문제가 발생하면 다음을 확인하세요:
1. [문제 해결 가이드](docs/TROUBLESHOOTING.md)
2. 각 컴포넌트의 README 파일
3. 로그 파일 (`docker-compose logs`)

---

**마지막 업데이트:** 2025-11-26
