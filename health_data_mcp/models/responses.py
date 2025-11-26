"""Response models for health data aggregation queries.

이 모듈은 MCP 서버의 응답 데이터 구조를 정의합니다.
Pydantic 모델을 사용하여 응답 검증 및 직렬화를 수행합니다.
"""

from typing import Any
from pydantic import BaseModel, Field, field_validator


class DateRange(BaseModel):
    """날짜 범위 모델
    
    Attributes:
        start: 시작 날짜 (YYYY-MM-DD)
        end: 종료 날짜 (YYYY-MM-DD)
    """
    start: str = Field(..., description="시작 날짜 (YYYY-MM-DD)")
    end: str = Field(..., description="종료 날짜 (YYYY-MM-DD)")


class WeekRange(BaseModel):
    """주 범위 모델
    
    Attributes:
        start_week: 시작 주 (YYYY-Www)
        end_week: 종료 주 (YYYY-Www)
    """
    start_week: str = Field(..., description="시작 주 (YYYY-Www)")
    end_week: str = Field(..., description="종료 주 (YYYY-Www)")


class MonthRange(BaseModel):
    """월 범위 모델
    
    Attributes:
        start_month: 시작 월 (YYYY-MM)
        end_month: 종료 월 (YYYY-MM)
    """
    start_month: str = Field(..., description="시작 월 (YYYY-MM)")
    end_month: str = Field(..., description="종료 월 (YYYY-MM)")


class AggregateRecord(BaseModel):
    """집계 레코드 모델
    
    일간, 주간, 월간 집계 데이터의 공통 필드를 포함합니다.
    
    Attributes:
        aggregation_date: 집계 날짜 (일간 집계용, 선택적)
        year: 연도 (주간/월간 집계용, 선택적)
        week_of_year: 주 번호 (주간 집계용, 선택적)
        week_start_date: 주 시작 날짜 (주간 집계용, 선택적)
        week_end_date: 주 종료 날짜 (주간 집계용, 선택적)
        month: 월 (월간 집계용, 선택적)
        month_start_date: 월 시작 날짜 (월간 집계용, 선택적)
        month_end_date: 월 종료 날짜 (월간 집계용, 선택적)
        days_in_month: 해당 월의 일수 (월간 집계용, 선택적)
        window_start: 윈도우 시작 시간
        window_end: 윈도우 종료 시간
        min_value: 최소값
        max_value: 최대값
        avg_value: 평균값
        sum_value: 합계값
        count: 레코드 수
        stddev_value: 표준편차 (선택적)
        first_value: 첫 번째 값 (선택적)
        last_value: 마지막 값 (선택적)
        daily_avg_of_avg: 일평균의 평균 (주간/월간 집계용, 선택적)
        record_count: 원본 레코드 수
        timezone: 타임존
    """
    # 일간 집계 필드
    aggregation_date: str | None = Field(None, description="집계 날짜 (YYYY-MM-DD)")
    
    # 주간/월간 집계 필드
    year: int | None = Field(None, description="연도")
    week_of_year: int | None = Field(None, description="주 번호 (1-53)")
    week_start_date: str | None = Field(None, description="주 시작 날짜")
    week_end_date: str | None = Field(None, description="주 종료 날짜")
    month: int | None = Field(None, description="월 (1-12)")
    month_start_date: str | None = Field(None, description="월 시작 날짜")
    month_end_date: str | None = Field(None, description="월 종료 날짜")
    days_in_month: int | None = Field(None, description="해당 월의 일수")
    
    # 공통 필드
    window_start: str = Field(..., description="윈도우 시작 시간 (ISO 8601)")
    window_end: str = Field(..., description="윈도우 종료 시간 (ISO 8601)")
    min_value: float | None = Field(None, description="최소값")
    max_value: float | None = Field(None, description="최대값")
    avg_value: float | None = Field(None, description="평균값")
    sum_value: float | None = Field(None, description="합계값")
    count: int = Field(..., description="집계된 레코드 수")
    stddev_value: float | None = Field(None, description="표준편차")
    first_value: float | None = Field(None, description="첫 번째 값")
    last_value: float | None = Field(None, description="마지막 값")
    daily_avg_of_avg: float | None = Field(None, description="일평균의 평균")
    record_count: int | None = Field(None, description="원본 레코드 수")
    timezone: str = Field(..., description="타임존")
    
    model_config = {
        "json_schema_extra": {
            "examples": [
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
            ]
        }
    }


class TopRecord(BaseModel):
    """상위 레코드 모델
    
    정렬된 상위 레코드의 정보를 포함합니다.
    
    Attributes:
        rank: 순위
        aggregation_date: 집계 날짜
        value: 정렬 기준 값
        min_value: 최소값
        max_value: 최대값
        avg_value: 평균값
        sum_value: 합계값
        count: 레코드 수
        stddev_value: 표준편차 (선택적)
    """
    rank: int = Field(..., description="순위", ge=1)
    aggregation_date: str | None = Field(None, description="집계 날짜 (YYYY-MM-DD)")
    value: float | None = Field(None, description="정렬 기준 값")
    min_value: float | None = Field(None, description="최소값")
    max_value: float | None = Field(None, description="최대값")
    avg_value: float | None = Field(None, description="평균값")
    sum_value: float | None = Field(None, description="합계값")
    count: int | None = Field(None, description="레코드 수")
    stddev_value: float | None = Field(None, description="표준편차")
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "rank": 1,
                    "aggregation_date": "2025-11-15",
                    "value": 18500.0,
                    "avg_value": 18500.0,
                    "max_value": 18500.0,
                    "count": 1
                }
            ]
        }
    }


class DailyAggregateResponse(BaseModel):
    """일간 집계 응답 모델
    
    특정 사용자의 일간 집계 데이터 조회 결과를 나타냅니다.
    
    Attributes:
        user_id: 사용자 ID
        data_type: 데이터 타입
        date_range: 날짜 범위
        records: 집계 레코드 리스트
        count: 레코드 수
    """
    user_id: str = Field(..., description="사용자 ID")
    data_type: str = Field(..., description="데이터 타입 (heartRate, steps 등)")
    date_range: DateRange = Field(..., description="조회한 날짜 범위")
    records: list[dict[str, Any]] = Field(
        default_factory=list,
        description="일간 집계 레코드 리스트"
    )
    count: int = Field(..., description="반환된 레코드 수", ge=0)
    
    @field_validator("records", mode="before")
    @classmethod
    def validate_records(cls, v: Any) -> list[dict[str, Any]]:
        """레코드 리스트 검증"""
        if not isinstance(v, list):
            raise ValueError("records must be a list")
        return v
    
    model_config = {
        "json_schema_extra": {
            "examples": [
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
                            "timezone": "Asia/Seoul"
                        }
                    ],
                    "count": 30
                }
            ]
        }
    }


class WeeklyAggregateResponse(BaseModel):
    """주간 집계 응답 모델
    
    특정 사용자의 주간 집계 데이터 조회 결과를 나타냅니다.
    
    Attributes:
        user_id: 사용자 ID
        data_type: 데이터 타입
        week_range: 주 범위
        records: 집계 레코드 리스트
        count: 레코드 수
    """
    user_id: str = Field(..., description="사용자 ID")
    data_type: str = Field(..., description="데이터 타입 (heartRate, steps 등)")
    week_range: WeekRange = Field(..., description="조회한 주 범위")
    records: list[dict[str, Any]] = Field(
        default_factory=list,
        description="주간 집계 레코드 리스트"
    )
    count: int = Field(..., description="반환된 레코드 수", ge=0)
    
    @field_validator("records", mode="before")
    @classmethod
    def validate_records(cls, v: Any) -> list[dict[str, Any]]:
        """레코드 리스트 검증"""
        if not isinstance(v, list):
            raise ValueError("records must be a list")
        return v
    
    model_config = {
        "json_schema_extra": {
            "examples": [
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
                            "daily_avg_of_avg": 10500.0,
                            "timezone": "Asia/Seoul"
                        }
                    ],
                    "count": 7
                }
            ]
        }
    }


class MonthlyAggregateResponse(BaseModel):
    """월간 집계 응답 모델
    
    특정 사용자의 월간 집계 데이터 조회 결과를 나타냅니다.
    
    Attributes:
        user_id: 사용자 ID
        data_type: 데이터 타입
        month_range: 월 범위
        records: 집계 레코드 리스트
        count: 레코드 수
    """
    user_id: str = Field(..., description="사용자 ID")
    data_type: str = Field(..., description="데이터 타입 (heartRate, steps 등)")
    month_range: MonthRange = Field(..., description="조회한 월 범위")
    records: list[dict[str, Any]] = Field(
        default_factory=list,
        description="월간 집계 레코드 리스트"
    )
    count: int = Field(..., description="반환된 레코드 수", ge=0)
    
    @field_validator("records", mode="before")
    @classmethod
    def validate_records(cls, v: Any) -> list[dict[str, Any]]:
        """레코드 리스트 검증"""
        if not isinstance(v, list):
            raise ValueError("records must be a list")
        return v
    
    model_config = {
        "json_schema_extra": {
            "examples": [
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
                            "daily_avg_of_avg": 74.2,
                            "timezone": "Asia/Seoul"
                        }
                    ],
                    "count": 6
                }
            ]
        }
    }


class TopRecordsResponse(BaseModel):
    """상위 레코드 응답 모델
    
    특정 기간 내 상위 레코드 조회 결과를 나타냅니다.
    
    Attributes:
        user_id: 사용자 ID
        data_type: 데이터 타입
        date_range: 날짜 범위
        sort_by: 정렬 기준
        order: 정렬 순서
        records: 상위 레코드 리스트
        count: 레코드 수
    """
    user_id: str = Field(..., description="사용자 ID")
    data_type: str = Field(..., description="데이터 타입 (heartRate, steps 등)")
    date_range: DateRange = Field(..., description="조회한 날짜 범위")
    sort_by: str = Field(..., description="정렬 기준 (max_value, avg_value, sum_value, count)")
    order: str = Field(..., description="정렬 순서 (asc, desc)")
    records: list[dict[str, Any]] = Field(
        default_factory=list,
        description="상위 레코드 리스트 (rank 포함)"
    )
    count: int = Field(..., description="반환된 레코드 수", ge=0)
    
    @field_validator("sort_by")
    @classmethod
    def validate_sort_by(cls, v: str) -> str:
        """정렬 기준 검증"""
        valid_values = ["max_value", "avg_value", "sum_value", "count"]
        if v not in valid_values:
            raise ValueError(f"sort_by must be one of: {', '.join(valid_values)}")
        return v
    
    @field_validator("order")
    @classmethod
    def validate_order(cls, v: str) -> str:
        """정렬 순서 검증"""
        valid_values = ["asc", "desc"]
        if v not in valid_values:
            raise ValueError(f"order must be one of: {', '.join(valid_values)}")
        return v
    
    @field_validator("records", mode="before")
    @classmethod
    def validate_records(cls, v: Any) -> list[dict[str, Any]]:
        """레코드 리스트 검증"""
        if not isinstance(v, list):
            raise ValueError("records must be a list")
        return v
    
    model_config = {
        "json_schema_extra": {
            "examples": [
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
                            "value": 18500.0,
                            "avg_value": 18500.0,
                            "max_value": 18500.0,
                            "count": 1
                        },
                        {
                            "rank": 2,
                            "aggregation_date": "2025-11-22",
                            "value": 17200.0,
                            "avg_value": 17200.0,
                            "max_value": 17200.0,
                            "count": 1
                        }
                    ],
                    "count": 10
                }
            ]
        }
    }


class ErrorDetails(BaseModel):
    """에러 상세 정보 모델
    
    Attributes:
        field: 오류가 발생한 필드 (선택적)
        provided: 제공된 값 (선택적)
        expected: 예상되는 값 또는 형식 (선택적)
    """
    field: str | None = Field(None, description="오류가 발생한 필드")
    provided: str | None = Field(None, description="제공된 값")
    expected: str | None = Field(None, description="예상되는 값 또는 형식")
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "field": "start_date",
                    "provided": "2025/11/17",
                    "expected": "YYYY-MM-DD"
                }
            ]
        }
    }


class ErrorResponse(BaseModel):
    """에러 응답 모델
    
    MCP 서버에서 발생한 오류 정보를 나타냅니다.
    
    Attributes:
        error: 에러 정보
            - type: 에러 타입 (ValidationError, NotFoundError, ConnectionError, InternalError)
            - message: 에러 메시지
            - details: 에러 상세 정보 (선택적)
    """
    error: dict[str, Any] = Field(
        ...,
        description="에러 정보"
    )
    
    @field_validator("error")
    @classmethod
    def validate_error(cls, v: dict[str, Any]) -> dict[str, Any]:
        """에러 정보 검증"""
        if not isinstance(v, dict):
            raise ValueError("error must be a dictionary")
        
        if "type" not in v:
            raise ValueError("error must contain 'type' field")
        
        if "message" not in v:
            raise ValueError("error must contain 'message' field")
        
        # 에러 타입 검증
        valid_types = [
            "ValidationError",
            "NotFoundError",
            "ConnectionError",
            "QueryError",
            "InternalError"
        ]
        if v["type"] not in valid_types:
            raise ValueError(f"error type must be one of: {', '.join(valid_types)}")
        
        return v
    
    model_config = {
        "json_schema_extra": {
            "examples": [
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
            ]
        }
    }
