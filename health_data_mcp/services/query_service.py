"""Query service for health data aggregations."""

from datetime import datetime, timedelta
from typing import Any

from health_data_mcp.config import Settings
from health_data_mcp.exceptions import ValidationError
from health_data_mcp.services.iceberg_client import IcebergClient, IcebergConnectionError, IcebergQueryError
from health_data_mcp.logging_config import get_logger


logger = get_logger(__name__)


class QueryService:
    """Iceberg 테이블 쿼리를 처리하는 서비스
    
    일간, 주간, 월간 집계 데이터를 조회하는 메서드를 제공합니다.
    날짜 범위 기본값 설정, 필터 생성, 결과 포맷팅을 담당합니다.
    
    Attributes:
        settings: MCP 서버 설정
        iceberg_client: Iceberg 클라이언트 인스턴스
    """
    
    def __init__(self, settings: Settings):
        """QueryService 초기화
        
        Args:
            settings: MCP 서버 설정
        """
        self.settings = settings
        self.iceberg_client = IcebergClient(settings)
        logger.info("QueryService initialized successfully")
    
    async def get_daily_aggregates(
        self,
        user_id: str,
        data_type: str,
        start_date: str | None = None,
        end_date: str | None = None
    ) -> dict[str, Any]:
        """일간 집계 데이터 조회
        
        특정 사용자의 일간 집계 데이터를 날짜 범위로 조회합니다.
        날짜 범위가 제공되지 않으면 최근 N일 데이터를 반환합니다.
        
        Args:
            user_id: 사용자 ID
            data_type: 데이터 타입 (heartRate, steps 등)
            start_date: 시작 날짜 (YYYY-MM-DD, 선택적)
            end_date: 종료 날짜 (YYYY-MM-DD, 선택적)
            
        Returns:
            일간 집계 데이터 응답
            {
                "user_id": str,
                "data_type": str,
                "date_range": {"start": str, "end": str},
                "records": list[dict],
                "count": int
            }
            
        Raises:
            ValueError: 잘못된 날짜 형식 또는 범위
            IcebergConnectionError: Iceberg 연결 오류
            IcebergQueryError: 쿼리 실행 오류
        """
        logger.info(
            f"Querying daily aggregates: user_id={user_id}, data_type={data_type}, "
            f"start_date={start_date}, end_date={end_date}"
        )
        
        # 필수 파라미터 검증
        self._validate_required_param(user_id, "user_id")
        self._validate_required_param(data_type, "data_type")
        
        # 날짜 범위 기본값 설정
        if not start_date or not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (
                datetime.now() - timedelta(days=self.settings.default_daily_range_days)
            ).strftime("%Y-%m-%d")
            logger.debug(f"Using default date range: {start_date} to {end_date}")
        
        # 날짜 형식 검증
        self._validate_date_format(start_date, "start_date")
        self._validate_date_format(end_date, "end_date")
        
        # 날짜 범위 유효성 검증
        self._validate_date_range(start_date, end_date)
        
        # 필터 생성
        filters = [
            ("user_id", "==", user_id),
            ("data_type", "==", data_type),
            ("aggregation_date", ">=", start_date),
            ("aggregation_date", "<=", end_date),
        ]
        
        # Iceberg 테이블 스캔
        try:
            table_name = self.settings.table_daily_agg
            results = self.iceberg_client.scan_table(
                table_name=table_name,
                filters=filters
            )
            
            logger.info(
                f"Daily aggregates query completed: "
                f"user_id={user_id}, data_type={data_type}, records={len(results)}"
            )
            
            # 결과 포맷팅
            return {
                "user_id": user_id,
                "data_type": data_type,
                "date_range": {
                    "start": start_date,
                    "end": end_date
                },
                "records": results,
                "count": len(results)
            }
            
        except (IcebergConnectionError, IcebergQueryError) as e:
            logger.error(f"Failed to query daily aggregates: {str(e)}")
            raise

    async def get_weekly_aggregates(
        self,
        user_id: str,
        data_type: str,
        start_week: str | None = None,
        end_week: str | None = None
    ) -> dict[str, Any]:
        """주간 집계 데이터 조회
        
        특정 사용자의 주간 집계 데이터를 주 범위로 조회합니다.
        주 범위가 제공되지 않으면 최근 N주 데이터를 반환합니다.
        
        Args:
            user_id: 사용자 ID
            data_type: 데이터 타입 (heartRate, steps 등)
            start_week: 시작 주 (YYYY-Www, 선택적, 예: 2025-W40)
            end_week: 종료 주 (YYYY-Www, 선택적, 예: 2025-W46)
            
        Returns:
            주간 집계 데이터 응답
            {
                "user_id": str,
                "data_type": str,
                "week_range": {"start_week": str, "end_week": str},
                "records": list[dict],
                "count": int
            }
            
        Raises:
            ValueError: 잘못된 주 형식 또는 범위
            IcebergConnectionError: Iceberg 연결 오류
            IcebergQueryError: 쿼리 실행 오류
        """
        logger.info(
            f"Querying weekly aggregates: user_id={user_id}, data_type={data_type}, "
            f"start_week={start_week}, end_week={end_week}"
        )
        
        # 필수 파라미터 검증
        self._validate_required_param(user_id, "user_id")
        self._validate_required_param(data_type, "data_type")
        
        # 주 범위 기본값 설정
        if not start_week or not end_week:
            current_date = datetime.now()
            end_year, end_week_num, _ = current_date.isocalendar()
            end_week = f"{end_year}-W{end_week_num:02d}"
            
            # 시작 주 계산 (N주 전)
            start_date = current_date - timedelta(weeks=self.settings.default_weekly_range_weeks)
            start_year, start_week_num, _ = start_date.isocalendar()
            start_week = f"{start_year}-W{start_week_num:02d}"
            
            logger.debug(f"Using default week range: {start_week} to {end_week}")
        
        # 주 형식 검증
        self._validate_week_format(start_week, "start_week")
        self._validate_week_format(end_week, "end_week")
        
        # 주를 year, week_of_year로 파싱
        start_year, start_week_num = self._parse_week_string(start_week)
        end_year, end_week_num = self._parse_week_string(end_week)
        
        # 주 범위 유효성 검증
        self._validate_week_range(start_year, start_week_num, end_year, end_week_num)
        
        # 필터 생성 (year와 week_of_year 범위로 필터링)
        filters = [
            ("user_id", "==", user_id),
            ("data_type", "==", data_type),
        ]
        
        # 연도가 같은 경우
        if start_year == end_year:
            filters.extend([
                ("year", "==", start_year),
                ("week_of_year", ">=", start_week_num),
                ("week_of_year", "<=", end_week_num),
            ])
        else:
            # 연도가 다른 경우 - 모든 데이터를 가져온 후 필터링
            # (Iceberg의 OR 조건 처리를 위해)
            filters.extend([
                ("year", ">=", start_year),
                ("year", "<=", end_year),
            ])
        
        # Iceberg 테이블 스캔
        try:
            table_name = self.settings.table_weekly_agg
            results = self.iceberg_client.scan_table(
                table_name=table_name,
                filters=filters
            )
            
            # 연도가 다른 경우 추가 필터링
            if start_year != end_year:
                results = [
                    r for r in results
                    if (r["year"] == start_year and r["week_of_year"] >= start_week_num) or
                       (r["year"] == end_year and r["week_of_year"] <= end_week_num) or
                       (start_year < r["year"] < end_year)
                ]
            
            logger.info(
                f"Weekly aggregates query completed: "
                f"user_id={user_id}, data_type={data_type}, records={len(results)}"
            )
            
            # 결과 포맷팅
            return {
                "user_id": user_id,
                "data_type": data_type,
                "week_range": {
                    "start_week": start_week,
                    "end_week": end_week
                },
                "records": results,
                "count": len(results)
            }
            
        except (IcebergConnectionError, IcebergQueryError) as e:
            logger.error(f"Failed to query weekly aggregates: {str(e)}")
            raise
    
    async def get_monthly_aggregates(
        self,
        user_id: str,
        data_type: str,
        start_month: str | None = None,
        end_month: str | None = None
    ) -> dict[str, Any]:
        """월간 집계 데이터 조회
        
        특정 사용자의 월간 집계 데이터를 월 범위로 조회합니다.
        월 범위가 제공되지 않으면 최근 N개월 데이터를 반환합니다.
        
        Args:
            user_id: 사용자 ID
            data_type: 데이터 타입 (heartRate, steps 등)
            start_month: 시작 월 (YYYY-MM, 선택적, 예: 2025-06)
            end_month: 종료 월 (YYYY-MM, 선택적, 예: 2025-11)
            
        Returns:
            월간 집계 데이터 응답
            {
                "user_id": str,
                "data_type": str,
                "month_range": {"start_month": str, "end_month": str},
                "records": list[dict],
                "count": int
            }
            
        Raises:
            ValueError: 잘못된 월 형식 또는 범위
            IcebergConnectionError: Iceberg 연결 오류
            IcebergQueryError: 쿼리 실행 오류
        """
        logger.info(
            f"Querying monthly aggregates: user_id={user_id}, data_type={data_type}, "
            f"start_month={start_month}, end_month={end_month}"
        )
        
        # 필수 파라미터 검증
        self._validate_required_param(user_id, "user_id")
        self._validate_required_param(data_type, "data_type")
        
        # 월 범위 기본값 설정
        if not start_month or not end_month:
            current_date = datetime.now()
            end_month = current_date.strftime("%Y-%m")
            
            # 시작 월 계산 (N개월 전)
            # 월 계산을 위해 연도와 월을 분리하여 처리
            months_back = self.settings.default_monthly_range_months
            start_date = current_date
            for _ in range(months_back):
                # 이전 달로 이동
                if start_date.month == 1:
                    start_date = start_date.replace(year=start_date.year - 1, month=12)
                else:
                    start_date = start_date.replace(month=start_date.month - 1)
            
            start_month = start_date.strftime("%Y-%m")
            logger.debug(f"Using default month range: {start_month} to {end_month}")
        
        # 월 형식 검증
        self._validate_month_format(start_month, "start_month")
        self._validate_month_format(end_month, "end_month")
        
        # 월을 year, month로 파싱
        start_year, start_month_num = self._parse_month_string(start_month)
        end_year, end_month_num = self._parse_month_string(end_month)
        
        # 월 범위 유효성 검증
        self._validate_month_range(start_year, start_month_num, end_year, end_month_num)
        
        # 필터 생성
        filters = [
            ("user_id", "==", user_id),
            ("data_type", "==", data_type),
        ]
        
        # 연도가 같은 경우
        if start_year == end_year:
            filters.extend([
                ("year", "==", start_year),
                ("month", ">=", start_month_num),
                ("month", "<=", end_month_num),
            ])
        else:
            # 연도가 다른 경우 - 모든 데이터를 가져온 후 필터링
            filters.extend([
                ("year", ">=", start_year),
                ("year", "<=", end_year),
            ])
        
        # Iceberg 테이블 스캔
        try:
            table_name = self.settings.table_monthly_agg
            results = self.iceberg_client.scan_table(
                table_name=table_name,
                filters=filters
            )
            
            # 연도가 다른 경우 추가 필터링
            if start_year != end_year:
                results = [
                    r for r in results
                    if (r["year"] == start_year and r["month"] >= start_month_num) or
                       (r["year"] == end_year and r["month"] <= end_month_num) or
                       (start_year < r["year"] < end_year)
                ]
            
            logger.info(
                f"Monthly aggregates query completed: "
                f"user_id={user_id}, data_type={data_type}, records={len(results)}"
            )
            
            # 결과 포맷팅
            return {
                "user_id": user_id,
                "data_type": data_type,
                "month_range": {
                    "start_month": start_month,
                    "end_month": end_month
                },
                "records": results,
                "count": len(results)
            }
            
        except (IcebergConnectionError, IcebergQueryError) as e:
            logger.error(f"Failed to query monthly aggregates: {str(e)}")
            raise

    async def get_top_records(
        self,
        user_id: str,
        data_type: str,
        start_date: str | None = None,
        end_date: str | None = None,
        sort_by: str = "max_value",
        order: str = "desc",
        limit: int = 10
    ) -> dict[str, Any]:
        """특정 기간 내 상위 레코드 조회
        
        일간 집계 데이터를 특정 메트릭 기준으로 정렬하여 상위 N개를 반환합니다.
        예: "최근 30일 중 가장 많이 걸었던 날 10개"
        
        Args:
            user_id: 사용자 ID
            data_type: 데이터 타입 (heartRate, steps 등)
            start_date: 시작 날짜 (YYYY-MM-DD, 선택적)
            end_date: 종료 날짜 (YYYY-MM-DD, 선택적)
            sort_by: 정렬 기준 (max_value, avg_value, sum_value, count)
            order: 정렬 순서 (asc, desc)
            limit: 반환할 레코드 수 (기본 10개, 최대 10000개)
            
        Returns:
            상위 레코드 응답
            {
                "user_id": str,
                "data_type": str,
                "date_range": {"start": str, "end": str},
                "sort_by": str,
                "order": str,
                "records": list[dict],  # rank 정보 포함
                "count": int
            }
            
        Raises:
            ValueError: 잘못된 파라미터 (날짜, sort_by, order, limit)
            IcebergConnectionError: Iceberg 연결 오류
            IcebergQueryError: 쿼리 실행 오류
        """
        logger.info(
            f"Querying top records: user_id={user_id}, data_type={data_type}, "
            f"start_date={start_date}, end_date={end_date}, sort_by={sort_by}, "
            f"order={order}, limit={limit}"
        )
        
        # 필수 파라미터 검증
        self._validate_required_param(user_id, "user_id")
        self._validate_required_param(data_type, "data_type")
        
        # 파라미터 검증
        self._validate_sort_by(sort_by)
        self._validate_order(order)
        self._validate_limit(limit)
        
        # 날짜 범위 기본값 설정
        if not start_date or not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (
                datetime.now() - timedelta(days=self.settings.default_daily_range_days)
            ).strftime("%Y-%m-%d")
            logger.debug(f"Using default date range: {start_date} to {end_date}")
        
        # 날짜 형식 검증
        self._validate_date_format(start_date, "start_date")
        self._validate_date_format(end_date, "end_date")
        
        # 날짜 범위 유효성 검증
        self._validate_date_range(start_date, end_date)
        
        # 필터 생성
        filters = [
            ("user_id", "==", user_id),
            ("data_type", "==", data_type),
            ("aggregation_date", ">=", start_date),
            ("aggregation_date", "<=", end_date),
        ]
        
        # Iceberg 테이블 스캔
        try:
            table_name = self.settings.table_daily_agg
            results = self.iceberg_client.scan_table(
                table_name=table_name,
                filters=filters
            )
            
            # 정렬
            reverse = (order == "desc")
            sorted_results = sorted(
                results,
                key=lambda x: x.get(sort_by, 0) if x.get(sort_by) is not None else 0,
                reverse=reverse
            )
            
            # Limit 적용
            top_results = sorted_results[:limit]
            
            # 순위 정보 추가
            ranked_results = []
            for rank, record in enumerate(top_results, start=1):
                ranked_record = {
                    "rank": rank,
                    "aggregation_date": record.get("aggregation_date"),
                    "value": record.get(sort_by),
                    "min_value": record.get("min_value"),
                    "max_value": record.get("max_value"),
                    "avg_value": record.get("avg_value"),
                    "sum_value": record.get("sum_value"),
                    "count": record.get("count"),
                    "stddev_value": record.get("stddev_value"),
                }
                ranked_results.append(ranked_record)
            
            logger.info(
                f"Top records query completed: "
                f"user_id={user_id}, data_type={data_type}, "
                f"sort_by={sort_by}, order={order}, records={len(ranked_results)}"
            )
            
            # 결과 포맷팅
            return {
                "user_id": user_id,
                "data_type": data_type,
                "date_range": {
                    "start": start_date,
                    "end": end_date
                },
                "sort_by": sort_by,
                "order": order,
                "records": ranked_results,
                "count": len(ranked_results)
            }
            
        except (IcebergConnectionError, IcebergQueryError) as e:
            logger.error(f"Failed to query top records: {str(e)}")
            raise

    # Validation helper methods
    
    def _validate_required_param(self, value: str | None, field_name: str) -> None:
        """필수 파라미터 검증
        
        Args:
            value: 파라미터 값
            field_name: 필드 이름 (오류 메시지용)
            
        Raises:
            ValidationError: 필수 파라미터가 누락되거나 빈 문자열인 경우
        """
        if value is None or (isinstance(value, str) and value.strip() == ""):
            raise ValidationError(
                f"Required parameter '{field_name}' is missing or empty",
                details={
                    "field": field_name,
                    "provided": str(value) if value is not None else "null",
                    "expected": "non-empty string"
                }
            )
    
    def _validate_date_format(self, date_str: str, field_name: str) -> None:
        """날짜 형식 검증 (YYYY-MM-DD)
        
        Args:
            date_str: 날짜 문자열
            field_name: 필드 이름 (오류 메시지용)
            
        Raises:
            ValidationError: 잘못된 날짜 형식
        """
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError as e:
            raise ValidationError(
                f"Invalid date format for {field_name}",
                details={
                    "field": field_name,
                    "provided": date_str,
                    "expected": "YYYY-MM-DD (e.g., 2025-11-17)"
                }
            ) from e
    
    def _validate_date_range(self, start_date: str, end_date: str) -> None:
        """날짜 범위 유효성 검증
        
        Args:
            start_date: 시작 날짜 (YYYY-MM-DD)
            end_date: 종료 날짜 (YYYY-MM-DD)
            
        Raises:
            ValidationError: 시작 날짜가 종료 날짜보다 늦은 경우
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start > end:
            raise ValidationError(
                "Invalid date range: start_date must be before or equal to end_date",
                details={
                    "field": "date_range",
                    "provided": f"start={start_date}, end={end_date}",
                    "expected": "start_date <= end_date"
                }
            )
    
    def _validate_week_format(self, week_str: str, field_name: str) -> None:
        """주 형식 검증 (YYYY-Www)
        
        Args:
            week_str: 주 문자열
            field_name: 필드 이름 (오류 메시지용)
            
        Raises:
            ValidationError: 잘못된 주 형식
        """
        try:
            parts = week_str.split("-W")
            if len(parts) != 2:
                raise ValueError("Invalid format")
            
            year = int(parts[0])
            week = int(parts[1])
            
            if year < 1900 or year > 2100:
                raise ValueError("Year out of range")
            
            if week < 1 or week > 53:
                raise ValueError("Week must be between 1 and 53")
                
        except (ValueError, IndexError) as e:
            raise ValidationError(
                f"Invalid week format for {field_name}",
                details={
                    "field": field_name,
                    "provided": week_str,
                    "expected": "YYYY-Www (e.g., 2025-W46, week 1-53)"
                }
            ) from e
    
    def _parse_week_string(self, week_str: str) -> tuple[int, int]:
        """주 문자열을 year, week_of_year로 파싱
        
        Args:
            week_str: 주 문자열 (YYYY-Www)
            
        Returns:
            (year, week_of_year) 튜플
        """
        parts = week_str.split("-W")
        year = int(parts[0])
        week = int(parts[1])
        return year, week
    
    def _validate_week_range(
        self,
        start_year: int,
        start_week: int,
        end_year: int,
        end_week: int
    ) -> None:
        """주 범위 유효성 검증
        
        Args:
            start_year: 시작 연도
            start_week: 시작 주
            end_year: 종료 연도
            end_week: 종료 주
            
        Raises:
            ValidationError: 시작 주가 종료 주보다 늦은 경우
        """
        if start_year > end_year:
            raise ValidationError(
                "Invalid week range: start week must be before or equal to end week",
                details={
                    "field": "week_range",
                    "provided": f"start={start_year}-W{start_week:02d}, end={end_year}-W{end_week:02d}",
                    "expected": "start_week <= end_week"
                }
            )
        
        if start_year == end_year and start_week > end_week:
            raise ValidationError(
                "Invalid week range: start week must be before or equal to end week",
                details={
                    "field": "week_range",
                    "provided": f"start={start_year}-W{start_week:02d}, end={end_year}-W{end_week:02d}",
                    "expected": "start_week <= end_week"
                }
            )
    
    def _validate_month_format(self, month_str: str, field_name: str) -> None:
        """월 형식 검증 (YYYY-MM)
        
        Args:
            month_str: 월 문자열
            field_name: 필드 이름 (오류 메시지용)
            
        Raises:
            ValidationError: 잘못된 월 형식
        """
        try:
            datetime.strptime(month_str, "%Y-%m")
        except ValueError as e:
            raise ValidationError(
                f"Invalid month format for {field_name}",
                details={
                    "field": field_name,
                    "provided": month_str,
                    "expected": "YYYY-MM (e.g., 2025-11)"
                }
            ) from e
    
    def _parse_month_string(self, month_str: str) -> tuple[int, int]:
        """월 문자열을 year, month로 파싱
        
        Args:
            month_str: 월 문자열 (YYYY-MM)
            
        Returns:
            (year, month) 튜플
        """
        date = datetime.strptime(month_str, "%Y-%m")
        return date.year, date.month
    
    def _validate_month_range(
        self,
        start_year: int,
        start_month: int,
        end_year: int,
        end_month: int
    ) -> None:
        """월 범위 유효성 검증
        
        Args:
            start_year: 시작 연도
            start_month: 시작 월
            end_year: 종료 연도
            end_month: 종료 월
            
        Raises:
            ValidationError: 시작 월이 종료 월보다 늦은 경우
        """
        if start_year > end_year:
            raise ValidationError(
                "Invalid month range: start month must be before or equal to end month",
                details={
                    "field": "month_range",
                    "provided": f"start={start_year}-{start_month:02d}, end={end_year}-{end_month:02d}",
                    "expected": "start_month <= end_month"
                }
            )
        
        if start_year == end_year and start_month > end_month:
            raise ValidationError(
                "Invalid month range: start month must be before or equal to end month",
                details={
                    "field": "month_range",
                    "provided": f"start={start_year}-{start_month:02d}, end={end_year}-{end_month:02d}",
                    "expected": "start_month <= end_month"
                }
            )
    
    def _validate_sort_by(self, sort_by: str) -> None:
        """정렬 기준 검증
        
        Args:
            sort_by: 정렬 기준
            
        Raises:
            ValidationError: 잘못된 정렬 기준
        """
        valid_sort_fields = ["max_value", "avg_value", "sum_value", "count"]
        if sort_by not in valid_sort_fields:
            raise ValidationError(
                f"Invalid sort_by value",
                details={
                    "field": "sort_by",
                    "provided": sort_by,
                    "expected": f"one of: {', '.join(valid_sort_fields)}"
                }
            )
    
    def _validate_order(self, order: str) -> None:
        """정렬 순서 검증
        
        Args:
            order: 정렬 순서
            
        Raises:
            ValidationError: 잘못된 정렬 순서
        """
        valid_orders = ["asc", "desc"]
        if order not in valid_orders:
            raise ValidationError(
                f"Invalid order value",
                details={
                    "field": "order",
                    "provided": order,
                    "expected": f"one of: {', '.join(valid_orders)}"
                }
            )
    
    def _validate_limit(self, limit: int) -> None:
        """Limit 값 검증
        
        Args:
            limit: 반환할 레코드 수
            
        Raises:
            ValidationError: 잘못된 limit 값
        """
        if not isinstance(limit, int):
            raise ValidationError(
                f"Invalid limit type",
                details={
                    "field": "limit",
                    "provided": f"{type(limit).__name__}: {limit}",
                    "expected": "integer"
                }
            )
        
        if limit < 1 or limit > 10000:
            raise ValidationError(
                f"Invalid limit value",
                details={
                    "field": "limit",
                    "provided": str(limit),
                    "expected": "integer between 1 and 10000"
                }
            )
