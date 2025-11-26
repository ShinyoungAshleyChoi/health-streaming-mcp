"""Configuration management for Health Data MCP Server."""

from pydantic import Field, field_validator, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """MCP 서버 설정
    
    환경 변수를 통해 Iceberg 카탈로그, S3, 테이블 이름 및 기본 쿼리 범위를 설정합니다.
    모든 필수 설정은 환경 변수로 제공되어야 하며, 누락 시 ValidationError가 발생합니다.
    """
    
    # Iceberg 카탈로그 설정
    iceberg_catalog_name: str = Field(
        alias="ICEBERG_CATALOG_NAME",
        description="Iceberg 카탈로그 이름"
    )
    iceberg_catalog_uri: str = Field(
        alias="ICEBERG_CATALOG_URI",
        description="Iceberg REST 카탈로그 URI (예: http://localhost:8181)"
    )
    iceberg_warehouse: str = Field(
        alias="ICEBERG_WAREHOUSE",
        description="Iceberg 웨어하우스 경로 (예: s3://warehouse)"
    )
    iceberg_database: str = Field(
        alias="ICEBERG_DATABASE",
        description="Iceberg 데이터베이스 이름"
    )
    
    # S3 설정
    s3_endpoint: str = Field(
        alias="S3_ENDPOINT",
        description="S3 엔드포인트 URL (예: http://localhost:9000)"
    )
    s3_access_key: str = Field(
        alias="S3_ACCESS_KEY",
        description="S3 액세스 키"
    )
    s3_secret_key: str = Field(
        alias="S3_SECRET_KEY",
        description="S3 시크릿 키"
    )
    
    # 테이블 이름 설정
    table_daily_agg: str = Field(
        default="health_data_daily_agg",
        description="일간 집계 테이블 이름"
    )
    table_weekly_agg: str = Field(
        default="health_data_weekly_agg",
        description="주간 집계 테이블 이름"
    )
    table_monthly_agg: str = Field(
        default="health_data_monthly_agg",
        description="월간 집계 테이블 이름"
    )
    
    # 기본 쿼리 범위 설정
    default_daily_range_days: int = Field(
        default=30,
        ge=1,
        le=365,
        description="일간 집계 기본 조회 범위 (일 단위, 1-365)"
    )
    default_weekly_range_weeks: int = Field(
        default=12,
        ge=1,
        le=52,
        description="주간 집계 기본 조회 범위 (주 단위, 1-52)"
    )
    default_monthly_range_months: int = Field(
        default=6,
        ge=1,
        le=24,
        description="월간 집계 기본 조회 범위 (월 단위, 1-24)"
    )
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False
    )
    
    @field_validator("iceberg_catalog_uri", "s3_endpoint")
    @classmethod
    def validate_uri(cls, v: str, info) -> str:
        """URI 형식 검증"""
        if not v.startswith(("http://", "https://")):
            raise ValueError(
                f"{info.field_name}은 http:// 또는 https://로 시작해야 합니다"
            )
        return v
    
    @field_validator("iceberg_warehouse")
    @classmethod
    def validate_warehouse(cls, v: str) -> str:
        """웨어하우스 경로 검증"""
        if not v.startswith("s3://"):
            raise ValueError("iceberg_warehouse는 s3://로 시작해야 합니다")
        return v
    
    @field_validator("table_daily_agg", "table_weekly_agg", "table_monthly_agg")
    @classmethod
    def validate_table_name(cls, v: str, info) -> str:
        """테이블 이름 검증"""
        if not v or not v.strip():
            raise ValueError(f"{info.field_name}은 비어있을 수 없습니다")
        if not v.replace("_", "").isalnum():
            raise ValueError(
                f"{info.field_name}은 알파벳, 숫자, 언더스코어만 포함할 수 있습니다"
            )
        return v.strip()
    
    def get_full_table_name(self, table_type: str) -> str:
        """전체 테이블 이름 반환 (database.table)
        
        Args:
            table_type: 테이블 타입 ("daily", "weekly", "monthly")
            
        Returns:
            전체 테이블 이름 (예: "health_data.health_data_daily_agg")
            
        Raises:
            ValueError: 잘못된 table_type
        """
        table_map = {
            "daily": self.table_daily_agg,
            "weekly": self.table_weekly_agg,
            "monthly": self.table_monthly_agg,
        }
        
        if table_type not in table_map:
            raise ValueError(
                f"잘못된 table_type: {table_type}. "
                f"'daily', 'weekly', 'monthly' 중 하나여야 합니다"
            )
        
        return f"{self.iceberg_database}.{table_map[table_type]}"
    
    def get_iceberg_properties(self) -> dict[str, str]:
        """Iceberg 카탈로그 연결에 필요한 속성 반환
        
        Returns:
            Iceberg 카탈로그 속성 딕셔너리
        """
        return {
            "uri": self.iceberg_catalog_uri,
            "warehouse": self.iceberg_warehouse,
            "s3.endpoint": self.s3_endpoint,
            "s3.access-key-id": self.s3_access_key,
            "s3.secret-access-key": self.s3_secret_key,
            "s3.path-style-access": "true",
        }
