"""Iceberg client for querying health data aggregation tables."""

from typing import Any

from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError, NoSuchNamespaceError
from pyiceberg.table import Table
import pyarrow as pa

from health_data_mcp.config import Settings
from health_data_mcp.logging_config import get_logger


logger = get_logger(__name__)


class IcebergConnectionError(Exception):
    """Iceberg 카탈로그 연결 오류"""
    pass


class IcebergQueryError(Exception):
    """Iceberg 쿼리 실행 오류"""
    pass


class IcebergClient:
    """PyIceberg를 사용한 Iceberg 테이블 클라이언트
    
    Iceberg REST 카탈로그에 연결하여 테이블을 로드하고 스캔하는 기능을 제공합니다.
    필터링, 제한, PyArrow 테이블 변환을 지원합니다.
    
    Attributes:
        settings: MCP 서버 설정
        catalog: Iceberg REST 카탈로그 인스턴스
    """
    
    def __init__(self, settings: Settings):
        """IcebergClient 초기화
        
        Args:
            settings: MCP 서버 설정
            
        Raises:
            IcebergConnectionError: 카탈로그 연결 실패 시
        """
        self.settings = settings
        self.catalog = self._init_catalog()
        self._verify_connection()
    
    def _init_catalog(self) -> RestCatalog:
        """Iceberg REST 카탈로그 초기화
        
        Returns:
            초기화된 RestCatalog 인스턴스
            
        Raises:
            IcebergConnectionError: 카탈로그 초기화 실패 시
        """
        try:
            logger.info(
                f"Initializing Iceberg catalog: {self.settings.iceberg_catalog_name} "
                f"at {self.settings.iceberg_catalog_uri}"
            )
            
            catalog = load_catalog(
                name=self.settings.iceberg_catalog_name,
                **self.settings.get_iceberg_properties()
            )
            
            logger.info("Iceberg catalog initialized successfully")
            return catalog
            
        except Exception as e:
            error_msg = (
                f"Failed to initialize Iceberg catalog "
                f"'{self.settings.iceberg_catalog_name}' "
                f"at '{self.settings.iceberg_catalog_uri}': {str(e)}"
            )
            logger.error(error_msg, exc_info=True)
            raise IcebergConnectionError(error_msg) from e
    
    def _verify_connection(self) -> None:
        """카탈로그 연결 검증
        
        데이터베이스 존재 여부를 확인하여 연결이 정상적으로 작동하는지 검증합니다.
        
        Raises:
            IcebergConnectionError: 연결 검증 실패 시
        """
        try:
            logger.info(f"Verifying connection to database: {self.settings.iceberg_database}")
            
            # 데이터베이스(네임스페이스) 존재 확인
            namespaces = self.catalog.list_namespaces()
            database_tuple = (self.settings.iceberg_database,)
            
            if database_tuple not in namespaces:
                error_msg = (
                    f"Database '{self.settings.iceberg_database}' not found in catalog. "
                    f"Available namespaces: {[ns[0] for ns in namespaces]}"
                )
                logger.error(error_msg)
                raise IcebergConnectionError(error_msg)
            
            logger.info(f"Connection verified. Database '{self.settings.iceberg_database}' exists")
            
        except IcebergConnectionError:
            raise
        except Exception as e:
            error_msg = (
                f"Failed to verify connection to database "
                f"'{self.settings.iceberg_database}': {str(e)}"
            )
            logger.error(error_msg, exc_info=True)
            raise IcebergConnectionError(error_msg) from e

    def load_table(self, table_name: str) -> Table:
        """Iceberg 테이블 로드
        
        Args:
            table_name: 테이블 이름 (database.table 형식 또는 table만)
            
        Returns:
            로드된 Iceberg Table 인스턴스
            
        Raises:
            IcebergConnectionError: 테이블 로드 실패 시
        """
        try:
            # 테이블 이름에 데이터베이스가 포함되지 않은 경우 추가
            if "." not in table_name:
                full_table_name = f"{self.settings.iceberg_database}.{table_name}"
            else:
                full_table_name = table_name
            
            logger.info(f"Loading table: {full_table_name}")
            table = self.catalog.load_table(full_table_name)
            logger.info(f"Table loaded successfully: {full_table_name}")
            
            return table
            
        except NoSuchTableError as e:
            error_msg = f"Table '{table_name}' not found in catalog: {str(e)}"
            logger.error(error_msg)
            raise IcebergConnectionError(error_msg) from e
        except NoSuchNamespaceError as e:
            error_msg = f"Namespace not found for table '{table_name}': {str(e)}"
            logger.error(error_msg)
            raise IcebergConnectionError(error_msg) from e
        except Exception as e:
            error_msg = f"Failed to load table '{table_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise IcebergConnectionError(error_msg) from e
    
    def scan_table(
        self,
        table_name: str,
        filters: list[tuple[str, str, Any]] | None = None,
        limit: int | None = None,
        selected_fields: list[str] | None = None
    ) -> list[dict[str, Any]]:
        """Iceberg 테이블 스캔
        
        테이블을 스캔하고 필터를 적용하여 결과를 반환합니다.
        PyArrow 테이블을 딕셔너리 리스트로 변환합니다.
        
        Args:
            table_name: 테이블 이름
            filters: 필터 조건 리스트 [(field, operator, value), ...]
                     지원되는 연산자: "==", "!=", ">", ">=", "<", "<="
            limit: 결과 제한 (선택적)
            selected_fields: 선택할 필드 리스트 (선택적, None이면 모든 필드)
        
        Returns:
            레코드 딕셔너리 리스트
            
        Raises:
            IcebergConnectionError: 테이블 로드 실패 시
            IcebergQueryError: 쿼리 실행 실패 시
            
        Example:
            >>> client = IcebergClient(settings)
            >>> filters = [
            ...     ("user_id", "==", "user-123"),
            ...     ("aggregation_date", ">=", "2025-11-01"),
            ...     ("aggregation_date", "<=", "2025-11-30")
            ... ]
            >>> results = client.scan_table("health_data_daily_agg", filters=filters, limit=100)
        """
        try:
            # 테이블 로드
            table = self.load_table(table_name)
            
            # 스캔 시작
            scan = table.scan()
            
            # 필드 선택
            if selected_fields:
                logger.debug(f"Selecting fields: {selected_fields}")
                scan = scan.select(*selected_fields)
            
            # 필터 적용
            if filters:
                logger.debug(f"Applying {len(filters)} filters")
                scan = self._apply_filters(scan, filters)
            
            # Limit 적용 (스캔 레벨에서)
            if limit:
                logger.debug(f"Applying limit: {limit}")
                scan = scan.limit(limit)
            
            # PyArrow 테이블로 변환
            logger.debug("Converting to PyArrow table")
            arrow_table = scan.to_arrow()
            
            # 딕셔너리 리스트로 변환
            results = self._arrow_to_dict_list(arrow_table)
            
            logger.info(
                f"Scan completed successfully. "
                f"Table: {table_name}, Records: {len(results)}"
            )
            
            return results
            
        except IcebergConnectionError:
            raise
        except Exception as e:
            error_msg = f"Failed to scan table '{table_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise IcebergQueryError(error_msg) from e
    
    def _apply_filters(self, scan, filters: list[tuple[str, str, Any]]):
        """스캔에 필터 적용
        
        Args:
            scan: Iceberg 스캔 객체
            filters: 필터 조건 리스트 [(field, operator, value), ...]
            
        Returns:
            필터가 적용된 스캔 객체
            
        Raises:
            ValueError: 지원되지 않는 연산자
        """
        from pyiceberg.expressions import (
            EqualTo, NotEqualTo, 
            GreaterThan, GreaterThanOrEqual,
            LessThan, LessThanOrEqual
        )
        
        for field, operator, value in filters:
            logger.debug(f"Applying filter: {field} {operator} {value}")
            
            if operator == "==":
                scan = scan.filter(EqualTo(field, value))
            elif operator == "!=":
                scan = scan.filter(NotEqualTo(field, value))
            elif operator == ">":
                scan = scan.filter(GreaterThan(field, value))
            elif operator == ">=":
                scan = scan.filter(GreaterThanOrEqual(field, value))
            elif operator == "<":
                scan = scan.filter(LessThan(field, value))
            elif operator == "<=":
                scan = scan.filter(LessThanOrEqual(field, value))
            else:
                raise ValueError(
                    f"Unsupported operator: {operator}. "
                    f"Supported operators: ==, !=, >, >=, <, <="
                )
        
        return scan
    
    def _arrow_to_dict_list(self, arrow_table: pa.Table) -> list[dict[str, Any]]:
        """PyArrow 테이블을 딕셔너리 리스트로 변환
        
        Args:
            arrow_table: PyArrow 테이블
            
        Returns:
            딕셔너리 리스트
        """
        try:
            # PyArrow의 to_pylist()를 사용하여 변환
            # 이 메서드는 날짜, 타임스탬프 등을 자동으로 Python 객체로 변환
            results = arrow_table.to_pylist()
            
            # 타임스탬프를 문자열로 변환 (JSON 직렬화를 위해)
            for record in results:
                for key, value in record.items():
                    if hasattr(value, 'isoformat'):
                        # datetime 객체를 ISO 형식 문자열로 변환
                        record[key] = value.isoformat()
            
            return results
            
        except Exception as e:
            error_msg = f"Failed to convert PyArrow table to dict list: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise IcebergQueryError(error_msg) from e
    
    def list_tables(self) -> list[str]:
        """데이터베이스의 모든 테이블 목록 조회
        
        Returns:
            테이블 이름 리스트
            
        Raises:
            IcebergConnectionError: 테이블 목록 조회 실패 시
        """
        try:
            namespace = (self.settings.iceberg_database,)
            tables = self.catalog.list_tables(namespace)
            table_names = [f"{table[0]}.{table[1]}" for table in tables]
            
            logger.info(f"Found {len(table_names)} tables in database '{self.settings.iceberg_database}'")
            return table_names
            
        except Exception as e:
            error_msg = (
                f"Failed to list tables in database "
                f"'{self.settings.iceberg_database}': {str(e)}"
            )
            logger.error(error_msg, exc_info=True)
            raise IcebergConnectionError(error_msg) from e
