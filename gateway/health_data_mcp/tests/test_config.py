"""Tests for configuration management."""

import pytest
from pydantic import ValidationError as PydanticValidationError

from health_data_mcp.config import Settings


class TestSettings:
    """Tests for Settings configuration class."""
    
    def test_settings_with_valid_env_vars(self, monkeypatch):
        """Test settings initialization with valid environment variables."""
        # Set valid environment variables
        monkeypatch.setenv("ICEBERG_CATALOG_NAME", "test_catalog")
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "s3://test-warehouse")
        monkeypatch.setenv("ICEBERG_DATABASE", "test_db")
        monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
        monkeypatch.setenv("S3_ACCESS_KEY", "test_access_key")
        monkeypatch.setenv("S3_SECRET_KEY", "test_secret_key")
        
        settings = Settings()
        
        assert settings.iceberg_catalog_name == "test_catalog"
        assert settings.iceberg_catalog_uri == "http://localhost:8181"
        assert settings.iceberg_warehouse == "s3://test-warehouse"
        assert settings.iceberg_database == "test_db"
        assert settings.s3_endpoint == "http://localhost:9000"
        assert settings.s3_access_key == "test_access_key"
        assert settings.s3_secret_key == "test_secret_key"
    
    def test_settings_default_values(self, monkeypatch):
        """Test settings default values for optional fields."""
        # Set required environment variables
        monkeypatch.setenv("ICEBERG_CATALOG_NAME", "test_catalog")
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "s3://test-warehouse")
        monkeypatch.setenv("ICEBERG_DATABASE", "test_db")
        monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
        monkeypatch.setenv("S3_ACCESS_KEY", "test_access_key")
        monkeypatch.setenv("S3_SECRET_KEY", "test_secret_key")
        
        settings = Settings()
        
        # Check default values
        assert settings.table_daily_agg == "health_data_daily_agg"
        assert settings.table_weekly_agg == "health_data_weekly_agg"
        assert settings.table_monthly_agg == "health_data_monthly_agg"
        assert settings.default_daily_range_days == 30
        assert settings.default_weekly_range_weeks == 12
        assert settings.default_monthly_range_months == 6
    
    def test_settings_missing_required_field(self, monkeypatch):
        """Test settings validation fails when required field is missing."""
        # Set only some required fields
        monkeypatch.setenv("ICEBERG_CATALOG_NAME", "test_catalog")
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
        # Missing other required fields
        
        with pytest.raises(PydanticValidationError):
            Settings()
    
    def test_settings_invalid_uri_format(self, monkeypatch):
        """Test settings validation fails with invalid URI format."""
        monkeypatch.setenv("ICEBERG_CATALOG_NAME", "test_catalog")
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "invalid-uri")  # Invalid format
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "s3://test-warehouse")
        monkeypatch.setenv("ICEBERG_DATABASE", "test_db")
        monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
        monkeypatch.setenv("S3_ACCESS_KEY", "test_access_key")
        monkeypatch.setenv("S3_SECRET_KEY", "test_secret_key")
        
        with pytest.raises(PydanticValidationError) as exc_info:
            Settings()
        
        assert "http://" in str(exc_info.value) or "https://" in str(exc_info.value)
    
    def test_settings_invalid_warehouse_format(self, monkeypatch):
        """Test settings validation fails with invalid warehouse format."""
        monkeypatch.setenv("ICEBERG_CATALOG_NAME", "test_catalog")
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "invalid-warehouse")  # Invalid format
        monkeypatch.setenv("ICEBERG_DATABASE", "test_db")
        monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
        monkeypatch.setenv("S3_ACCESS_KEY", "test_access_key")
        monkeypatch.setenv("S3_SECRET_KEY", "test_secret_key")
        
        with pytest.raises(PydanticValidationError) as exc_info:
            Settings()
        
        assert "s3://" in str(exc_info.value)
    
    def test_settings_invalid_table_name(self, monkeypatch):
        """Test settings validation fails with invalid table name."""
        monkeypatch.setenv("ICEBERG_CATALOG_NAME", "test_catalog")
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "s3://test-warehouse")
        monkeypatch.setenv("ICEBERG_DATABASE", "test_db")
        monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
        monkeypatch.setenv("S3_ACCESS_KEY", "test_access_key")
        monkeypatch.setenv("S3_SECRET_KEY", "test_secret_key")
        monkeypatch.setenv("TABLE_DAILY_AGG", "invalid-table-name!")  # Invalid characters
        
        with pytest.raises(PydanticValidationError):
            Settings()
    
    def test_settings_invalid_range_values(self, monkeypatch):
        """Test settings validation fails with out-of-range values."""
        monkeypatch.setenv("ICEBERG_CATALOG_NAME", "test_catalog")
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "s3://test-warehouse")
        monkeypatch.setenv("ICEBERG_DATABASE", "test_db")
        monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
        monkeypatch.setenv("S3_ACCESS_KEY", "test_access_key")
        monkeypatch.setenv("S3_SECRET_KEY", "test_secret_key")
        monkeypatch.setenv("DEFAULT_DAILY_RANGE_DAYS", "500")  # Out of range (max 365)
        
        with pytest.raises(PydanticValidationError):
            Settings()
    
    def test_get_full_table_name(self, monkeypatch):
        """Test get_full_table_name method."""
        monkeypatch.setenv("ICEBERG_CATALOG_NAME", "test_catalog")
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "s3://test-warehouse")
        monkeypatch.setenv("ICEBERG_DATABASE", "test_db")
        monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
        monkeypatch.setenv("S3_ACCESS_KEY", "test_access_key")
        monkeypatch.setenv("S3_SECRET_KEY", "test_secret_key")
        
        settings = Settings()
        
        assert settings.get_full_table_name("daily") == "test_db.health_data_daily_agg"
        assert settings.get_full_table_name("weekly") == "test_db.health_data_weekly_agg"
        assert settings.get_full_table_name("monthly") == "test_db.health_data_monthly_agg"
    
    def test_get_full_table_name_invalid_type(self, monkeypatch):
        """Test get_full_table_name raises error for invalid table type."""
        monkeypatch.setenv("ICEBERG_CATALOG_NAME", "test_catalog")
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "s3://test-warehouse")
        monkeypatch.setenv("ICEBERG_DATABASE", "test_db")
        monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
        monkeypatch.setenv("S3_ACCESS_KEY", "test_access_key")
        monkeypatch.setenv("S3_SECRET_KEY", "test_secret_key")
        
        settings = Settings()
        
        with pytest.raises(ValueError) as exc_info:
            settings.get_full_table_name("invalid")
        
        assert "잘못된 table_type" in str(exc_info.value)
    
    def test_get_iceberg_properties(self, monkeypatch):
        """Test get_iceberg_properties method."""
        monkeypatch.setenv("ICEBERG_CATALOG_NAME", "test_catalog")
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "s3://test-warehouse")
        monkeypatch.setenv("ICEBERG_DATABASE", "test_db")
        monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
        monkeypatch.setenv("S3_ACCESS_KEY", "test_access_key")
        monkeypatch.setenv("S3_SECRET_KEY", "test_secret_key")
        
        settings = Settings()
        properties = settings.get_iceberg_properties()
        
        assert properties["uri"] == "http://localhost:8181"
        assert properties["warehouse"] == "s3://test-warehouse"
        assert properties["s3.endpoint"] == "http://localhost:9000"
        assert properties["s3.access-key-id"] == "test_access_key"
        assert properties["s3.secret-access-key"] == "test_secret_key"
        assert properties["s3.path-style-access"] == "true"
