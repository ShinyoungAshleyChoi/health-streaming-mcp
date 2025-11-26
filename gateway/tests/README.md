# Integration Tests

통합 테스트 환경 및 실행 가이드입니다.

## 테스트 구조

```
tests/
├── conftest.py                 # Pytest fixtures 및 설정
├── test_integration_api.py     # API 엔드포인트 통합 테스트
├── test_integration_kafka.py   # Kafka 메시지 검증 테스트
├── test_e2e_pipeline.py        # End-to-End 파이프라인 테스트
├── generate_test_data.py       # 테스트 데이터 생성 스크립트
└── README.md                   # 이 파일
```

## 사전 요구사항

테스트 실행을 위해 필요한 패키지를 설치합니다:

```bash
cd gateway
pip install pytest pytest-asyncio httpx confluent-kafka fastavro
```

## 테스트 환경 실행

### 1. 테스트용 Docker Compose 환경 시작

```bash
# 프로젝트 루트에서 실행
docker-compose -f docker-compose.test.yml up -d

# 서비스가 준비될 때까지 대기 (약 30초)
sleep 30
```

### 2. 서비스 상태 확인

```bash
# 모든 컨테이너가 healthy 상태인지 확인
docker-compose -f docker-compose.test.yml ps

# Gateway health check
curl http://localhost:3001/health
```

## 테스트 실행

### 전체 테스트 실행

```bash
cd gateway
pytest tests/ -v
```

### 특정 테스트 파일 실행

```bash
# API 통합 테스트만 실행
pytest tests/test_integration_api.py -v

# Kafka 통합 테스트만 실행
pytest tests/test_integration_kafka.py -v

# E2E 테스트만 실행
pytest tests/test_e2e_pipeline.py -v
```

### 특정 테스트 케이스 실행

```bash
# 특정 테스트 클래스 실행
pytest tests/test_integration_api.py::TestHealthDataAPI -v

# 특정 테스트 메서드 실행
pytest tests/test_integration_api.py::TestHealthDataAPI::test_post_valid_health_data -v
```

### 상세 로그와 함께 실행

```bash
pytest tests/ -v -s --log-cli-level=DEBUG
```

## 테스트 데이터 생성

테스트 데이터를 생성하려면:

```bash
cd gateway/tests
python generate_test_data.py
```

생성되는 파일:
- `test_data_valid.json` - 유효한 테스트 데이터 (100개 레코드)
- `test_data_invalid.json` - 검증 실패용 데이터
- `test_data_high_volume.json` - 대용량 테스트 데이터 (1000개 레코드)

## 테스트 카테고리

### 1. API 통합 테스트 (`test_integration_api.py`)

- ✅ 유효한 데이터 POST 요청
- ✅ 잘못된 JSON 형식 처리
- ✅ 필수 필드 누락 검증
- ✅ 데이터 타입 검증
- ✅ Request ID 헤더 처리
- ✅ 배치 요청 처리
- ✅ Health check 엔드포인트
- ✅ Metrics 엔드포인트

### 2. Kafka 통합 테스트 (`test_integration_kafka.py`)

- ✅ Avro 포맷 메시지 검증
- ✅ 메시지 키 (userId) 검증
- ✅ 파티션 라우팅 검증
- ✅ 메시지 헤더 검증
- ✅ 타임스탬프 검증
- ✅ Producer acknowledgment
- ✅ Idempotent producer
- ✅ Dead Letter Queue

### 3. End-to-End 테스트 (`test_e2e_pipeline.py`)

- ✅ 전체 파이프라인 플로우 (API → Kafka)
- ✅ 다양한 데이터 타입 처리
- ✅ 고처리량 테스트
- ✅ 스키마 진화 (Schema Evolution)
- ✅ 선택적 필드 처리
- ✅ 하위 호환성 (Backward Compatibility)
- ✅ Failover 시나리오
- ✅ 동시 요청 처리
- ✅ 메시지 순서 보장
- ✅ 에러 후 복구

## 환경 변수

테스트 환경 변수를 커스터마이즈하려면:

```bash
export TEST_GATEWAY_URL=http://localhost:3001
export TEST_KAFKA_BROKERS=localhost:19095
export TEST_SCHEMA_REGISTRY_URL=http://localhost:8082

pytest tests/ -v
```

## 테스트 환경 정리

테스트 완료 후 환경을 정리합니다:

```bash
# 테스트 컨테이너 중지 및 제거
docker-compose -f docker-compose.test.yml down -v

# 볼륨까지 완전히 제거
docker-compose -f docker-compose.test.yml down -v --remove-orphans
```

## 트러블슈팅

### 테스트가 타임아웃되는 경우

```bash
# 서비스가 완전히 시작될 때까지 더 오래 대기
sleep 60

# 개별 서비스 로그 확인
docker-compose -f docker-compose.test.yml logs gateway-test
docker-compose -f docker-compose.test.yml logs kafka-broker-test
```

### Kafka 연결 실패

```bash
# Kafka 브로커 상태 확인
docker exec kafka-broker-test kafka-broker-api-versions --bootstrap-server localhost:9092

# 토픽 목록 확인
docker exec kafka-broker-test kafka-topics --bootstrap-server localhost:9092 --list
```

### Schema Registry 연결 실패

```bash
# Schema Registry 상태 확인
curl http://localhost:8082/subjects

# 등록된 스키마 확인
curl http://localhost:8082/subjects/health-data-value/versions
```

## CI/CD 통합

GitHub Actions 예제:

```yaml
name: Integration Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Start test environment
        run: docker-compose -f docker-compose.test.yml up -d
      
      - name: Wait for services
        run: sleep 60
      
      - name: Run tests
        run: |
          cd gateway
          pip install pytest pytest-asyncio httpx confluent-kafka fastavro
          pytest tests/ -v
      
      - name: Cleanup
        if: always()
        run: docker-compose -f docker-compose.test.yml down -v
```

## 성능 벤치마크

고처리량 테스트 실행:

```bash
pytest tests/test_e2e_pipeline.py::TestEndToEndPipeline::test_high_throughput_pipeline -v -s
```

예상 성능:
- 처리량: > 100 messages/second
- 응답 시간 (p95): < 500ms
- 메시지 전달 성공률: > 95%
