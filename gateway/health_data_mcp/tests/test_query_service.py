"""Tests for query service."""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock

from health_data_mcp.services.query_service import QueryService
from health_data_mcp.exceptions import ValidationError
from health_data_mcp.config import Settings


@pytest.fixture
def mock_settings(monkeypatch):
    """Create mock settings for testing."""
    monkeypatch.setenv("ICEBERG_CATALOG_NAME", "test_catalog")
    monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
    monkeypatch.setenv("ICEBERG_WAREHOUSE", "s3://test-warehouse")
    monkeypatch.setenv("ICEBERG_DATABASE", "test_db")
    monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("S3_ACCESS_KEY", "test_access_key")
    monkeypatch.setenv("S3_SECRET_KEY", "test_secret_key")
    return Settings()


@pytest.fixture
def mock_iceberg_client():
    """Create a mock Iceberg client."""
    client = Mock()
    client.scan_table = Mock(return_value=[])
    return client


class TestQueryService:
    """Tests for QueryService class."""
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    def test_init_success(self, mock_client_class, mock_settings):
        """Test successful QueryService initialization."""
        service = QueryService(mock_settings)
        
        assert service.settings == mock_settings
        mock_client_class.assert_called_once_with(mock_settings)
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    async def test_get_daily_aggregates_with_date_range(
        self, mock_client_class, mock_settings, mock_iceberg_client
    ):
        """Test get_daily_aggregates with explicit date range."""
        mock_client_class.return_value = mock_iceberg_client
        mock_iceberg_client.scan_table.return_value = [
            {
                "user_id": "user-1",
                "data_type": "heartRate",
                "aggregation_date": "2025-11-17",
                "avg_value": 75.0
            }
        ]
        
        service = QueryService(mock_settings)
        result = await service.get_daily_aggregates(
            user_id="user-1",
            data_type="heartRate",
            start_date="2025-11-01",
            end_date="2025-11-30"
        )
        
        assert result["user_id"] == "user-1"
        assert result["data_type"] == "heartRate"
        assert result["date_range"]["start"] == "2025-11-01"
        assert result["date_range"]["end"] == "2025-11-30"
        assert result["count"] == 1
        assert len(result["records"]) == 1
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    async def test_get_daily_aggregates_default_date_range(
        self, mock_client_class, mock_settings, mock_iceberg_client
    ):
        """Test get_daily_aggregates uses default date range when not provided."""
        mock_client_class.return_value = mock_iceberg_client
        mock_iceberg_client.scan_table.return_value = []
        
        service = QueryService(mock_settings)
        result = await service.get_daily_aggregates(
            user_id="user-1",
            data_type="heartRate"
        )
        
        # Verify default date range is used (last 30 days)
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        
        assert result["date_range"]["start"] == start_date
        assert result["date_range"]["end"] == end_date
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    async def test_get_daily_aggregates_missing_user_id(
        self, mock_client_class, mock_settings
    ):
        """Test get_daily_aggregates raises error when user_id is missing."""
        service = QueryService(mock_settings)
        
        with pytest.raises(ValidationError) as exc_info:
            await service.get_daily_aggregates(
                user_id="",
                data_type="heartRate"
            )
        
        assert "user_id" in str(exc_info.value)
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    async def test_get_daily_aggregates_invalid_date_format(
        self, mock_client_class, mock_settings
    ):
        """Test get_daily_aggregates raises error for invalid date format."""
        service = QueryService(mock_settings)
        
        with pytest.raises(ValidationError) as exc_info:
            await service.get_daily_aggregates(
                user_id="user-1",
                data_type="heartRate",
                start_date="2025/11/01",  # Invalid format
                end_date="2025-11-30"
            )
        
        assert "Invalid date format" in str(exc_info.value)
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    async def test_get_daily_aggregates_invalid_date_range(
        self, mock_client_class, mock_settings
    ):
        """Test get_daily_aggregates raises error when start_date > end_date."""
        service = QueryService(mock_settings)
        
        with pytest.raises(ValidationError) as exc_info:
            await service.get_daily_aggregates(
                user_id="user-1",
                data_type="heartRate",
                start_date="2025-11-30",
                end_date="2025-11-01"  # Before start_date
            )
        
        assert "Invalid date range" in str(exc_info.value)
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    async def test_get_weekly_aggregates_with_week_range(
        self, mock_client_class, mock_settings, mock_iceberg_client
    ):
        """Test get_weekly_aggregates with explicit week range."""
        mock_client_class.return_value = mock_iceberg_client
        mock_iceberg_client.scan_table.return_value = [
            {
                "user_id": "user-1",
                "data_type": "steps",
                "year": 2025,
                "week_of_year": 46,
                "avg_value": 10000.0
            }
        ]
        
        service = QueryService(mock_settings)
        result = await service.get_weekly_aggregates(
            user_id="user-1",
            data_type="steps",
            start_week="2025-W40",
            end_week="2025-W46"
        )
        
        assert result["user_id"] == "user-1"
        assert result["data_type"] == "steps"
        assert result["week_range"]["start_week"] == "2025-W40"
        assert result["week_range"]["end_week"] == "2025-W46"
        assert result["count"] == 1
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    async def test_get_weekly_aggregates_default_week_range(
        self, mock_client_class, mock_settings, mock_iceberg_client
    ):
        """Test get_weekly_aggregates uses default week range when not provided."""
        mock_client_class.return_value = mock_iceberg_client
        mock_iceberg_client.scan_table.return_value = []
        
        service = QueryService(mock_settings)
        result = await service.get_weekly_aggregates(
            user_id="user-1",
            data_type="steps"
        )
        
        # Verify default week range is used (last 12 weeks)
        current_date = datetime.now()
        end_year, end_week_num, _ = current_date.isocalendar()
        expected_end_week = f"{end_year}-W{end_week_num:02d}"
        
        assert result["week_range"]["end_week"] == expected_end_week
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    async def test_get_weekly_aggregates_invalid_week_format(
        self, mock_client_class, mock_settings
    ):
        """Test get_weekly_aggregates raises error for invalid week format."""
        service = QueryService(mock_settings)
        
        with pytest.raises(ValidationError) as exc_info:
            await service.get_weekly_aggregates(
                user_id="user-1",
                data_type="steps",
                start_week="2025-40",  # Invalid format
                end_week="2025-W46"
            )
        
        assert "Invalid week format" in str(exc_info.value)
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    async def test_get_monthly_aggregates_with_month_range(
        self, mock_client_class, mock_settings, mock_iceberg_client
    ):
        """Test get_monthly_aggregates with explicit month range."""
        mock_client_class.return_value = mock_iceberg_client
        mock_iceberg_client.scan_table.return_value = [
            {
                "user_id": "user-1",
                "data_type": "heartRate",
                "year": 2025,
                "month": 11,
                "avg_value": 75.0
            }
        ]
        
        service = QueryService(mock_settings)
        result = await service.get_monthly_aggregates(
            user_id="user-1",
            data_type="heartRate",
            start_month="2025-06",
            end_month="2025-11"
        )
        
        assert result["user_id"] == "user-1"
        assert result["data_type"] == "heartRate"
        assert result["month_range"]["start_month"] == "2025-06"
        assert result["month_range"]["end_month"] == "2025-11"
        assert result["count"] == 1
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    async def test_get_monthly_aggregates_default_month_range(
        self, mock_client_class, mock_settings, mock_iceberg_client
    ):
        """Test get_monthly_aggregates uses default month range when not provided."""
        mock_client_class.return_value = mock_iceberg_client
        mock_iceberg_client.scan_table.return_value = []
        
        service = QueryService(mock_settings)
        result = await service.get_monthly_aggregates(
            user_id="user-1",
            data_type="heartRate"
        )
        
        # Verify default month range is used (last 6 months)
        end_month = datetime.now().strftime("%Y-%m")
        assert result["month_range"]["end_month"] == end_month
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    async def test_get_monthly_aggregates_invalid_month_format(
        self, mock_client_class, mock_settings
    ):
        """Test get_monthly_aggregates raises error for invalid month format."""
        service = QueryService(mock_settings)
        
        with pytest.raises(ValidationError) as exc_info:
            await service.get_monthly_aggregates(
                user_id="user-1",
                data_type="heartRate",
                start_month="2025/11",  # Invalid format
                end_month="2025-11"
            )
        
        assert "Invalid month format" in str(exc_info.value)
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    async def test_get_top_records_with_parameters(
        self, mock_client_class, mock_settings, mock_iceberg_client
    ):
        """Test get_top_records with all parameters."""
        mock_client_class.return_value = mock_iceberg_client
        mock_iceberg_client.scan_table.return_value = [
            {
                "user_id": "user-1",
                "data_type": "steps",
                "aggregation_date": "2025-11-17",
                "max_value": 15000.0,
                "avg_value": 12000.0,
                "sum_value": 15000.0,
                "count": 1,
                "min_value": 15000.0,
                "stddev_value": 0.0
            },
            {
                "user_id": "user-1",
                "data_type": "steps",
                "aggregation_date": "2025-11-16",
                "max_value": 10000.0,
                "avg_value": 8000.0,
                "sum_value": 10000.0,
                "count": 1,
                "min_value": 10000.0,
                "stddev_value": 0.0
            }
        ]
        
        service = QueryService(mock_settings)
        result = await service.get_top_records(
            user_id="user-1",
            data_type="steps",
            start_date="2025-11-01",
            end_date="2025-11-30",
            sort_by="max_value",
            order="desc",
            limit=5
        )
        
        assert result["user_id"] == "user-1"
        assert result["data_type"] == "steps"
        assert result["sort_by"] == "max_value"
        assert result["order"] == "desc"
        assert result["count"] == 2
        
        # Verify sorting (descending by max_value)
        assert result["records"][0]["rank"] == 1
        assert result["records"][0]["value"] == 15000.0
        assert result["records"][1]["rank"] == 2
        assert result["records"][1]["value"] == 10000.0
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    async def test_get_top_records_ascending_order(
        self, mock_client_class, mock_settings, mock_iceberg_client
    ):
        """Test get_top_records with ascending order."""
        mock_client_class.return_value = mock_iceberg_client
        mock_iceberg_client.scan_table.return_value = [
            {
                "user_id": "user-1",
                "data_type": "steps",
                "aggregation_date": "2025-11-17",
                "avg_value": 12000.0,
                "max_value": 15000.0,
                "sum_value": 15000.0,
                "count": 1,
                "min_value": 15000.0,
                "stddev_value": 0.0
            },
            {
                "user_id": "user-1",
                "data_type": "steps",
                "aggregation_date": "2025-11-16",
                "avg_value": 8000.0,
                "max_value": 10000.0,
                "sum_value": 10000.0,
                "count": 1,
                "min_value": 10000.0,
                "stddev_value": 0.0
            }
        ]
        
        service = QueryService(mock_settings)
        result = await service.get_top_records(
            user_id="user-1",
            data_type="steps",
            sort_by="avg_value",
            order="asc",
            limit=10
        )
        
        # Verify sorting (ascending by avg_value)
        assert result["records"][0]["rank"] == 1
        assert result["records"][0]["value"] == 8000.0
        assert result["records"][1]["rank"] == 2
        assert result["records"][1]["value"] == 12000.0
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    async def test_get_top_records_invalid_sort_by(
        self, mock_client_class, mock_settings
    ):
        """Test get_top_records raises error for invalid sort_by."""
        service = QueryService(mock_settings)
        
        with pytest.raises(ValidationError) as exc_info:
            await service.get_top_records(
                user_id="user-1",
                data_type="steps",
                sort_by="invalid_field"
            )
        
        assert "Invalid sort_by value" in str(exc_info.value)
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    async def test_get_top_records_invalid_order(
        self, mock_client_class, mock_settings
    ):
        """Test get_top_records raises error for invalid order."""
        service = QueryService(mock_settings)
        
        with pytest.raises(ValidationError) as exc_info:
            await service.get_top_records(
                user_id="user-1",
                data_type="steps",
                order="invalid"
            )
        
        assert "Invalid order value" in str(exc_info.value)
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    async def test_get_top_records_invalid_limit(
        self, mock_client_class, mock_settings
    ):
        """Test get_top_records raises error for invalid limit."""
        service = QueryService(mock_settings)
        
        with pytest.raises(ValidationError) as exc_info:
            await service.get_top_records(
                user_id="user-1",
                data_type="steps",
                limit=20000  # Exceeds max limit
            )
        
        assert "Invalid limit value" in str(exc_info.value)


class TestQueryServiceValidation:
    """Tests for QueryService validation methods."""
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    def test_validate_required_param_valid(self, mock_client_class, mock_settings):
        """Test _validate_required_param with valid value."""
        service = QueryService(mock_settings)
        # Should not raise exception
        service._validate_required_param("valid_value", "test_field")
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    def test_validate_required_param_empty(self, mock_client_class, mock_settings):
        """Test _validate_required_param with empty value."""
        service = QueryService(mock_settings)
        
        with pytest.raises(ValidationError):
            service._validate_required_param("", "test_field")
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    def test_validate_date_format_valid(self, mock_client_class, mock_settings):
        """Test _validate_date_format with valid date."""
        service = QueryService(mock_settings)
        # Should not raise exception
        service._validate_date_format("2025-11-17", "test_date")
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    def test_validate_date_format_invalid(self, mock_client_class, mock_settings):
        """Test _validate_date_format with invalid date."""
        service = QueryService(mock_settings)
        
        with pytest.raises(ValidationError):
            service._validate_date_format("2025/11/17", "test_date")
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    def test_validate_week_format_valid(self, mock_client_class, mock_settings):
        """Test _validate_week_format with valid week."""
        service = QueryService(mock_settings)
        # Should not raise exception
        service._validate_week_format("2025-W46", "test_week")
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    def test_validate_week_format_invalid(self, mock_client_class, mock_settings):
        """Test _validate_week_format with invalid week."""
        service = QueryService(mock_settings)
        
        with pytest.raises(ValidationError):
            service._validate_week_format("2025-46", "test_week")
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    def test_validate_month_format_valid(self, mock_client_class, mock_settings):
        """Test _validate_month_format with valid month."""
        service = QueryService(mock_settings)
        # Should not raise exception
        service._validate_month_format("2025-11", "test_month")
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    def test_validate_month_format_invalid(self, mock_client_class, mock_settings):
        """Test _validate_month_format with invalid month."""
        service = QueryService(mock_settings)
        
        with pytest.raises(ValidationError):
            service._validate_month_format("2025/11", "test_month")
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    def test_parse_week_string(self, mock_client_class, mock_settings):
        """Test _parse_week_string."""
        service = QueryService(mock_settings)
        
        year, week = service._parse_week_string("2025-W46")
        assert year == 2025
        assert week == 46
    
    @patch("health_data_mcp.services.query_service.IcebergClient")
    def test_parse_month_string(self, mock_client_class, mock_settings):
        """Test _parse_month_string."""
        service = QueryService(mock_settings)
        
        year, month = service._parse_month_string("2025-11")
        assert year == 2025
        assert month == 11
