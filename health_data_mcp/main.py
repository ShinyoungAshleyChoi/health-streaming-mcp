"""MCP Server for querying health data aggregations from Iceberg tables."""

import time
from typing import Any

from fastmcp import FastMCP
from pydantic import ValidationError as PydanticValidationError

from health_data_mcp.config import Settings
from health_data_mcp.exceptions import ValidationError
from health_data_mcp.services.query_service import QueryService
from health_data_mcp.services.iceberg_client import (
    IcebergConnectionError,
    IcebergQueryError,
)
from health_data_mcp.logging_config import (
    setup_logging,
    get_logger,
    log_tool_call,
    log_tool_result,
    log_error,
)


# Configure structured logging
setup_logging()
logger = get_logger(__name__)

# Create FastMCP instance
mcp = FastMCP("health-data-aggregates")

# Initialize settings and query service
try:
    settings = Settings()
    query_service = QueryService(settings)
    
    logger.info("=" * 60)
    logger.info("Health Data MCP Server initialized successfully")
    logger.info("=" * 60)
    logger.info(f"Catalog URI: {settings.iceberg_catalog_uri}")
    logger.info(f"Database: {settings.iceberg_database}")
    logger.info(f"Tables:")
    logger.info(f"  - Daily: {settings.table_daily_agg}")
    logger.info(f"  - Weekly: {settings.table_weekly_agg}")
    logger.info(f"  - Monthly: {settings.table_monthly_agg}")
    logger.info(f"Default ranges:")
    logger.info(f"  - Daily: {settings.default_daily_range_days} days")
    logger.info(f"  - Weekly: {settings.default_weekly_range_weeks} weeks")
    logger.info(f"  - Monthly: {settings.default_monthly_range_months} months")
    logger.info("=" * 60)
    logger.info("Available tools:")
    logger.info("  - get_daily_aggregates")
    logger.info("  - get_weekly_aggregates")
    logger.info("  - get_monthly_aggregates")
    logger.info("  - get_top_records")
    logger.info("=" * 60)
    
except PydanticValidationError as e:
    logger.error("Configuration validation failed:")
    for error in e.errors():
        logger.error(f"  - {error['loc'][0]}: {error['msg']}")
    raise
except Exception as e:
    logger.error(f"Failed to initialize MCP server: {str(e)}", exc_info=True)
    raise


@mcp.tool()
async def get_daily_aggregates(
    user_id: str,
    data_type: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    """
    특정 사용자의 일간 집계 데이터를 조회합니다.
    
    일간 집계 테이블에서 사용자의 건강 데이터를 날짜 범위로 조회합니다.
    날짜 범위를 지정하지 않으면 최근 30일 데이터를 반환합니다.
    
    Args:
        user_id: 사용자 ID (필수)
        data_type: 데이터 타입 (필수, 예: heartRate, steps, distance)
        start_date: 시작 날짜 (선택, 형식: YYYY-MM-DD, 예: 2025-11-01)
        end_date: 종료 날짜 (선택, 형식: YYYY-MM-DD, 예: 2025-11-30)
    
    Returns:
        일간 집계 데이터 응답:
        {
            "user_id": str,
            "data_type": str,
            "date_range": {"start": str, "end": str},
            "records": [
                {
                    "aggregation_date": str,
                    "min_value": float,
                    "max_value": float,
                    "avg_value": float,
                    "sum_value": float,
                    "count": int,
                    "stddev_value": float,
                    ...
                }
            ],
            "count": int
        }
        
        오류 발생 시:
        {
            "error": {
                "type": str,
                "message": str,
                "details": dict (optional)
            }
        }
    """
    start_time = time.time()
    
    try:
        # 도구 호출 로깅
        log_tool_call(logger, "get_daily_aggregates", {
            "user_id": user_id,
            "data_type": data_type,
            "start_date": start_date,
            "end_date": end_date
        })
        
        result = await query_service.get_daily_aggregates(
            user_id=user_id,
            data_type=data_type,
            start_date=start_date,
            end_date=end_date
        )
        
        # 실행 결과 로깅
        duration_ms = (time.time() - start_time) * 1000
        log_tool_result(logger, "get_daily_aggregates", result['count'], duration_ms)
        
        return result
        
    except ValidationError as e:
        logger.warning(f"Validation error in get_daily_aggregates: {e.message}")
        return e.to_dict()
    except IcebergConnectionError as e:
        log_error(logger, e, {"tool": "get_daily_aggregates", "user_id": user_id, "data_type": data_type})
        return {
            "error": {
                "type": "ConnectionError",
                "message": f"Failed to connect to Iceberg catalog: {str(e)}"
            }
        }
    except IcebergQueryError as e:
        log_error(logger, e, {"tool": "get_daily_aggregates", "user_id": user_id, "data_type": data_type})
        return {
            "error": {
                "type": "QueryError",
                "message": f"Failed to execute query: {str(e)}"
            }
        }
    except Exception as e:
        log_error(logger, e, {"tool": "get_daily_aggregates", "user_id": user_id, "data_type": data_type})
        return {
            "error": {
                "type": "InternalError",
                "message": "An unexpected error occurred. Please check server logs."
            }
        }


@mcp.tool()
async def get_weekly_aggregates(
    user_id: str,
    data_type: str,
    start_week: str | None = None,
    end_week: str | None = None,
) -> dict[str, Any]:
    """
    특정 사용자의 주간 집계 데이터를 조회합니다.
    
    주간 집계 테이블에서 사용자의 건강 데이터를 주 범위로 조회합니다.
    주 범위를 지정하지 않으면 최근 12주 데이터를 반환합니다.
    
    Args:
        user_id: 사용자 ID (필수)
        data_type: 데이터 타입 (필수, 예: heartRate, steps, distance)
        start_week: 시작 주 (선택, 형식: YYYY-Www, 예: 2025-W40)
        end_week: 종료 주 (선택, 형식: YYYY-Www, 예: 2025-W46)
    
    Returns:
        주간 집계 데이터 응답:
        {
            "user_id": str,
            "data_type": str,
            "week_range": {"start_week": str, "end_week": str},
            "records": [
                {
                    "year": int,
                    "week_of_year": int,
                    "week_start_date": str,
                    "week_end_date": str,
                    "min_value": float,
                    "max_value": float,
                    "avg_value": float,
                    "daily_avg_of_avg": float,
                    ...
                }
            ],
            "count": int
        }
        
        오류 발생 시:
        {
            "error": {
                "type": str,
                "message": str,
                "details": dict (optional)
            }
        }
    """
    start_time = time.time()
    
    try:
        # 도구 호출 로깅
        log_tool_call(logger, "get_weekly_aggregates", {
            "user_id": user_id,
            "data_type": data_type,
            "start_week": start_week,
            "end_week": end_week
        })
        
        result = await query_service.get_weekly_aggregates(
            user_id=user_id,
            data_type=data_type,
            start_week=start_week,
            end_week=end_week
        )
        
        # 실행 결과 로깅
        duration_ms = (time.time() - start_time) * 1000
        log_tool_result(logger, "get_weekly_aggregates", result['count'], duration_ms)
        
        return result
        
    except ValidationError as e:
        logger.warning(f"Validation error in get_weekly_aggregates: {e.message}")
        return e.to_dict()
    except IcebergConnectionError as e:
        log_error(logger, e, {"tool": "get_weekly_aggregates", "user_id": user_id, "data_type": data_type})
        return {
            "error": {
                "type": "ConnectionError",
                "message": f"Failed to connect to Iceberg catalog: {str(e)}"
            }
        }
    except IcebergQueryError as e:
        log_error(logger, e, {"tool": "get_weekly_aggregates", "user_id": user_id, "data_type": data_type})
        return {
            "error": {
                "type": "QueryError",
                "message": f"Failed to execute query: {str(e)}"
            }
        }
    except Exception as e:
        log_error(logger, e, {"tool": "get_weekly_aggregates", "user_id": user_id, "data_type": data_type})
        return {
            "error": {
                "type": "InternalError",
                "message": "An unexpected error occurred. Please check server logs."
            }
        }


@mcp.tool()
async def get_monthly_aggregates(
    user_id: str,
    data_type: str,
    start_month: str | None = None,
    end_month: str | None = None,
) -> dict[str, Any]:
    """
    특정 사용자의 월간 집계 데이터를 조회합니다.
    
    월간 집계 테이블에서 사용자의 건강 데이터를 월 범위로 조회합니다.
    월 범위를 지정하지 않으면 최근 6개월 데이터를 반환합니다.
    
    Args:
        user_id: 사용자 ID (필수)
        data_type: 데이터 타입 (필수, 예: heartRate, steps, distance)
        start_month: 시작 월 (선택, 형식: YYYY-MM, 예: 2025-06)
        end_month: 종료 월 (선택, 형식: YYYY-MM, 예: 2025-11)
    
    Returns:
        월간 집계 데이터 응답:
        {
            "user_id": str,
            "data_type": str,
            "month_range": {"start_month": str, "end_month": str},
            "records": [
                {
                    "year": int,
                    "month": int,
                    "month_start_date": str,
                    "month_end_date": str,
                    "days_in_month": int,
                    "min_value": float,
                    "max_value": float,
                    "avg_value": float,
                    "daily_avg_of_avg": float,
                    ...
                }
            ],
            "count": int
        }
        
        오류 발생 시:
        {
            "error": {
                "type": str,
                "message": str,
                "details": dict (optional)
            }
        }
    """
    start_time = time.time()
    
    try:
        # 도구 호출 로깅
        log_tool_call(logger, "get_monthly_aggregates", {
            "user_id": user_id,
            "data_type": data_type,
            "start_month": start_month,
            "end_month": end_month
        })
        
        result = await query_service.get_monthly_aggregates(
            user_id=user_id,
            data_type=data_type,
            start_month=start_month,
            end_month=end_month
        )
        
        # 실행 결과 로깅
        duration_ms = (time.time() - start_time) * 1000
        log_tool_result(logger, "get_monthly_aggregates", result['count'], duration_ms)
        
        return result
        
    except ValidationError as e:
        logger.warning(f"Validation error in get_monthly_aggregates: {e.message}")
        return e.to_dict()
    except IcebergConnectionError as e:
        log_error(logger, e, {"tool": "get_monthly_aggregates", "user_id": user_id, "data_type": data_type})
        return {
            "error": {
                "type": "ConnectionError",
                "message": f"Failed to connect to Iceberg catalog: {str(e)}"
            }
        }
    except IcebergQueryError as e:
        log_error(logger, e, {"tool": "get_monthly_aggregates", "user_id": user_id, "data_type": data_type})
        return {
            "error": {
                "type": "QueryError",
                "message": f"Failed to execute query: {str(e)}"
            }
        }
    except Exception as e:
        log_error(logger, e, {"tool": "get_monthly_aggregates", "user_id": user_id, "data_type": data_type})
        return {
            "error": {
                "type": "InternalError",
                "message": "An unexpected error occurred. Please check server logs."
            }
        }


@mcp.tool()
async def get_top_records(
    user_id: str,
    data_type: str,
    start_date: str | None = None,
    end_date: str | None = None,
    sort_by: str = "max_value",
    order: str = "desc",
    limit: int = 10,
) -> dict[str, Any]:
    """
    특정 기간 내에서 특정 메트릭의 최고/최저 기록을 조회합니다.
    
    일간 집계 데이터를 특정 메트릭 기준으로 정렬하여 상위 N개를 반환합니다.
    예: "최근 30일 중 가장 많이 걸었던 날 10개"
    
    Args:
        user_id: 사용자 ID (필수)
        data_type: 데이터 타입 (필수, 예: heartRate, steps, distance)
        start_date: 시작 날짜 (선택, 형식: YYYY-MM-DD, 예: 2025-11-01)
        end_date: 종료 날짜 (선택, 형식: YYYY-MM-DD, 예: 2025-11-30)
        sort_by: 정렬 기준 (선택, 기본값: max_value)
                 가능한 값: max_value, avg_value, sum_value, count
        order: 정렬 순서 (선택, 기본값: desc)
               가능한 값: asc (오름차순), desc (내림차순)
        limit: 반환할 레코드 수 (선택, 기본값: 10, 범위: 1-10000)
    
    Returns:
        상위 레코드 응답:
        {
            "user_id": str,
            "data_type": str,
            "date_range": {"start": str, "end": str},
            "sort_by": str,
            "order": str,
            "records": [
                {
                    "rank": int,
                    "aggregation_date": str,
                    "value": float,  # sort_by 기준 값
                    "min_value": float,
                    "max_value": float,
                    "avg_value": float,
                    "sum_value": float,
                    "count": int,
                    "stddev_value": float
                }
            ],
            "count": int
        }
        
        오류 발생 시:
        {
            "error": {
                "type": str,
                "message": str,
                "details": dict (optional)
            }
        }
    """
    start_time = time.time()
    
    try:
        # 도구 호출 로깅
        log_tool_call(logger, "get_top_records", {
            "user_id": user_id,
            "data_type": data_type,
            "start_date": start_date,
            "end_date": end_date,
            "sort_by": sort_by,
            "order": order,
            "limit": limit
        })
        
        result = await query_service.get_top_records(
            user_id=user_id,
            data_type=data_type,
            start_date=start_date,
            end_date=end_date,
            sort_by=sort_by,
            order=order,
            limit=limit
        )
        
        # 실행 결과 로깅
        duration_ms = (time.time() - start_time) * 1000
        log_tool_result(logger, "get_top_records", result['count'], duration_ms)
        
        return result
        
    except ValidationError as e:
        logger.warning(f"Validation error in get_top_records: {e.message}")
        return e.to_dict()
    except IcebergConnectionError as e:
        log_error(logger, e, {"tool": "get_top_records", "user_id": user_id, "data_type": data_type})
        return {
            "error": {
                "type": "ConnectionError",
                "message": f"Failed to connect to Iceberg catalog: {str(e)}"
            }
        }
    except IcebergQueryError as e:
        log_error(logger, e, {"tool": "get_top_records", "user_id": user_id, "data_type": data_type})
        return {
            "error": {
                "type": "QueryError",
                "message": f"Failed to execute query: {str(e)}"
            }
        }
    except Exception as e:
        log_error(logger, e, {"tool": "get_top_records", "user_id": user_id, "data_type": data_type})
        return {
            "error": {
                "type": "InternalError",
                "message": "An unexpected error occurred. Please check server logs."
            }
        }


if __name__ == "__main__":
    mcp.run()
