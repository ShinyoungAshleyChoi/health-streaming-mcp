"""Integration tests for Health Data MCP Server.

These tests verify the end-to-end functionality of the MCP server,
including query flows, filter combinations, error handling, and MCP protocol.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import pyarrow as pa

from health_data_mcp.config import Settings
from health_data_mcp.services.query_service import QueryService
from health_data_mcp.services.iceberg_client import IcebergClient
from health_data_mcp.exceptions import ValidationError
from health_data_mcp.main import (
    get_daily_aggregates,
    get_weekly_aggregates,
    get_monthly_aggregates,
    get_top_records,
)


@pytest.fixture
def integration_settings(monkeypatch):
    """Create settings for integration tests."""
    monkeypatch.setenv("ICEBERG_CATALOG_NAME", "test_catalog")
    monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
    monkeypatch.setenv("ICEBERG_WAREHOUSE", "s3://test-warehouse")
    monkeypatch.setenv("ICEBERG_DATABASE", "test_db")
    monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("S3_ACCESS_KEY", "test_key")
    monkeypatch.setenv("S3_SECRET_KEY", "test_secret")
    return Settings()


@pytest.fixture
def mock_catalog_with_tables():
    """Create a mock catalog with test tables."""
    catalog = Mock()
    catalog.list_namespaces.return_value = [("test_db",)]
    
    # Mock table structure
    mock_table = Mock()
    mock_scan = Mock()
    mock_table.scan.return_value = mock_scan
    mock_scan.filter.return_value = mock_scan
    mock_scan.select.return_value = mock_scan
    mock_scan.limit.return_value = mock_scan
    
    catalog.load_table.return_value = mock_table
    
    return catalog, mock_table, mock_scan


@pytest.mark.integration
class TestDailyAggregatesIntegration:
    """Integration tests for daily aggregates query flow."""
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    async def test_full_daily_query_flow(
        self, mock_load_catalog, integration_settings, mock_catalog_with_tables
    ):
        """Test complete daily aggregates query flow from service to Iceberg."""
        catalog, mock_table, mock_scan = mock_catalog_with_tables
        mock_load_catalog.return_value = catalog
        
        # Create test data
        test_data = pa.table({
            "user_id": ["user-123", "user-123", "user-123"],
            "data_type": ["heartRate", "heartRate", "heartRate"],
            "aggregation_date": ["2025-11-15", "2025-11-16", "2025-11-17"],
            "window_start": ["2025-11-15T00:00:00+09:00"] * 3,
            "window_end": ["2025-11-15T23:59:59+09:00"] * 3,
            "min_value": [60.0, 62.0, 58.0],
            "max_value": [120.0, 125.0, 118.0],
            "avg_value": [75.5, 78.2, 73.1],
            "sum_value": [7550.0, 7820.0, 7310.0],
            "count": [100, 100, 100],
            "stddev_value": [12.3, 13.1, 11.8],
            "first_value": [65.0, 68.0, 62.0],
            "last_value": [70.0, 72.0, 68.0],
            "record_count": [100, 100, 100],
            "timezone": ["Asia/Seoul"] * 3
        })
        mock_scan.to_arrow.return_value = test_data
        
        # Execute query
        query_service = QueryService(integration_settings)
        result = await query_service.get_daily_aggregates(
            user_id="user-123",
            data_type="heartRate",
            start_date="2025-11-15",
            end_date="2025-11-17"
        )
        
        # Verify results
        assert result["user_id"] == "user-123"
        assert result["data_type"] == "heartRate"
        assert result["count"] == 3
        assert len(result["records"]) == 3
        assert result["records"][0]["aggregation_date"] == "2025-11-15"
        assert result["records"][0]["avg_value"] == 75.5

    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    async def test_daily_query_with_multiple_filters(
        self, mock_load_catalog, integration_settings, mock_catalog_with_tables
    ):
        """Test daily query with multiple filter combinations."""
        catalog, mock_table, mock_scan = mock_catalog_with_tables
        mock_load_catalog.return_value = catalog
        
        # Test data with multiple users and data types
        test_data = pa.table({
            "user_id": ["user-123"],
            "data_type": ["steps"],
            "aggregation_date": ["2025-11-17"],
            "window_start": ["2025-11-17T00:00:00+09:00"],
            "window_end": ["2025-11-17T23:59:59+09:00"],
            "min_value": [5000.0],
            "max_value": [15000.0],
            "avg_value": [10000.0],
            "sum_value": [10000.0],
            "count": [1],
            "stddev_value": [0.0],
            "first_value": [10000.0],
            "last_value": [10000.0],
            "record_count": [1],
            "timezone": ["Asia/Seoul"]
        })
        mock_scan.to_arrow.return_value = test_data
        
        query_service = QueryService(integration_settings)
        result = await query_service.get_daily_aggregates(
            user_id="user-123",
            data_type="steps",
            start_date="2025-11-17",
            end_date="2025-11-17"
        )
        
        assert result["count"] == 1
        assert result["records"][0]["data_type"] == "steps"
        # Verify filters were applied
        assert mock_scan.filter.call_count >= 4  # user_id, data_type, start_date, end_date


@pytest.mark.integration
class TestWeeklyAggregatesIntegration:
    """Integration tests for weekly aggregates query flow."""
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    async def test_full_weekly_query_flow(
        self, mock_load_catalog, integration_settings, mock_catalog_with_tables
    ):
        """Test complete weekly aggregates query flow."""
        catalog, mock_table, mock_scan = mock_catalog_with_tables
        mock_load_catalog.return_value = catalog
        
        test_data = pa.table({
            "user_id": ["user-123", "user-123"],
            "data_type": ["steps", "steps"],
            "year": [2025, 2025],
            "week_of_year": [45, 46],
            "week_start_date": ["2025-11-03", "2025-11-10"],
            "week_end_date": ["2025-11-09", "2025-11-16"],
            "window_start": ["2025-11-03T00:00:00+09:00", "2025-11-10T00:00:00+09:00"],
            "window_end": ["2025-11-09T23:59:59+09:00", "2025-11-16T23:59:59+09:00"],
            "min_value": [5000.0, 6000.0],
            "max_value": [15000.0, 16000.0],
            "avg_value": [10000.0, 11000.0],
            "sum_value": [70000.0, 77000.0],
            "count": [7, 7],
            "stddev_value": [2500.0, 2600.0],
            "daily_avg_of_avg": [10000.0, 11000.0],
            "record_count": [700, 770],
            "timezone": ["Asia/Seoul", "Asia/Seoul"]
        })
        mock_scan.to_arrow.return_value = test_data
        
        query_service = QueryService(integration_settings)
        result = await query_service.get_weekly_aggregates(
            user_id="user-123",
            data_type="steps",
            start_week="2025-W45",
            end_week="2025-W46"
        )
        
        assert result["user_id"] == "user-123"
        assert result["count"] == 2
        assert result["records"][0]["year"] == 2025
        assert result["records"][0]["week_of_year"] == 45


@pytest.mark.integration
class TestMonthlyAggregatesIntegration:
    """Integration tests for monthly aggregates query flow."""
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    async def test_full_monthly_query_flow(
        self, mock_load_catalog, integration_settings, mock_catalog_with_tables
    ):
        """Test complete monthly aggregates query flow."""
        catalog, mock_table, mock_scan = mock_catalog_with_tables
        mock_load_catalog.return_value = catalog
        
        test_data = pa.table({
            "user_id": ["user-123", "user-123"],
            "data_type": ["heartRate", "heartRate"],
            "year": [2025, 2025],
            "month": [10, 11],
            "month_start_date": ["2025-10-01", "2025-11-01"],
            "month_end_date": ["2025-10-31", "2025-11-30"],
            "days_in_month": [31, 30],
            "window_start": ["2025-10-01T00:00:00+09:00", "2025-11-01T00:00:00+09:00"],
            "window_end": ["2025-10-31T23:59:59+09:00", "2025-11-30T23:59:59+09:00"],
            "min_value": [55.0, 58.0],
            "max_value": [130.0, 128.0],
            "avg_value": [74.2, 75.1],
            "sum_value": [222600.0, 225300.0],
            "count": [3000, 3000],
            "stddev_value": [13.1, 12.8],
            "daily_avg_of_avg": [74.2, 75.1],
            "record_count": [3000, 3000],
            "timezone": ["Asia/Seoul", "Asia/Seoul"]
        })
        mock_scan.to_arrow.return_value = test_data
        
        query_service = QueryService(integration_settings)
        result = await query_service.get_monthly_aggregates(
            user_id="user-123",
            data_type="heartRate",
            start_month="2025-10",
            end_month="2025-11"
        )
        
        assert result["user_id"] == "user-123"
        assert result["count"] == 2
        assert result["records"][0]["year"] == 2025
        assert result["records"][0]["month"] == 10
        assert result["records"][0]["days_in_month"] == 31


@pytest.mark.integration
class TestTopRecordsIntegration:
    """Integration tests for top records query flow."""
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    async def test_full_top_records_query_flow(
        self, mock_load_catalog, integration_settings, mock_catalog_with_tables
    ):
        """Test complete top records query flow with sorting."""
        catalog, mock_table, mock_scan = mock_catalog_with_tables
        mock_load_catalog.return_value = catalog
        
        test_data = pa.table({
            "user_id": ["user-123"] * 5,
            "data_type": ["steps"] * 5,
            "aggregation_date": ["2025-11-13", "2025-11-14", "2025-11-15", "2025-11-16", "2025-11-17"],
            "window_start": ["2025-11-13T00:00:00+09:00"] * 5,
            "window_end": ["2025-11-13T23:59:59+09:00"] * 5,
            "min_value": [5000.0, 6000.0, 7000.0, 8000.0, 9000.0],
            "max_value": [18000.0, 15000.0, 12000.0, 10000.0, 8000.0],
            "avg_value": [12000.0, 11000.0, 10000.0, 9000.0, 8500.0],
            "sum_value": [18000.0, 15000.0, 12000.0, 10000.0, 8000.0],
            "count": [1, 1, 1, 1, 1],
            "stddev_value": [0.0] * 5,
            "first_value": [18000.0, 15000.0, 12000.0, 10000.0, 8000.0],
            "last_value": [18000.0, 15000.0, 12000.0, 10000.0, 8000.0],
            "record_count": [1] * 5,
            "timezone": ["Asia/Seoul"] * 5
        })
        mock_scan.to_arrow.return_value = test_data
        
        query_service = QueryService(integration_settings)
        result = await query_service.get_top_records(
            user_id="user-123",
            data_type="steps",
            start_date="2025-11-13",
            end_date="2025-11-17",
            sort_by="max_value",
            order="desc",
            limit=3
        )
        
        assert result["count"] == 3
        assert result["sort_by"] == "max_value"
        assert result["order"] == "desc"
        # Verify descending order
        assert result["records"][0]["rank"] == 1
        assert result["records"][0]["value"] == 18000.0
        assert result["records"][1]["rank"] == 2
        assert result["records"][1]["value"] == 15000.0
        assert result["records"][2]["rank"] == 3
        assert result["records"][2]["value"] == 12000.0

    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    async def test_top_records_ascending_order(
        self, mock_load_catalog, integration_settings, mock_catalog_with_tables
    ):
        """Test top records with ascending order."""
        catalog, mock_table, mock_scan = mock_catalog_with_tables
        mock_load_catalog.return_value = catalog
        
        test_data = pa.table({
            "user_id": ["user-123"] * 3,
            "data_type": ["heartRate"] * 3,
            "aggregation_date": ["2025-11-15", "2025-11-16", "2025-11-17"],
            "window_start": ["2025-11-15T00:00:00+09:00"] * 3,
            "window_end": ["2025-11-15T23:59:59+09:00"] * 3,
            "min_value": [60.0, 62.0, 58.0],
            "max_value": [120.0, 125.0, 118.0],
            "avg_value": [75.5, 78.2, 73.1],
            "sum_value": [7550.0, 7820.0, 7310.0],
            "count": [100, 100, 100],
            "stddev_value": [12.3, 13.1, 11.8],
            "first_value": [65.0, 68.0, 62.0],
            "last_value": [70.0, 72.0, 68.0],
            "record_count": [100, 100, 100],
            "timezone": ["Asia/Seoul"] * 3
        })
        mock_scan.to_arrow.return_value = test_data
        
        query_service = QueryService(integration_settings)
        result = await query_service.get_top_records(
            user_id="user-123",
            data_type="heartRate",
            sort_by="avg_value",
            order="asc",
            limit=10
        )
        
        # Verify ascending order
        assert result["records"][0]["value"] == 73.1
        assert result["records"][1]["value"] == 75.5
        assert result["records"][2]["value"] == 78.2


@pytest.mark.integration
class TestErrorCasesIntegration:
    """Integration tests for error handling."""
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    async def test_table_not_found_error(
        self, mock_load_catalog, integration_settings
    ):
        """Test error handling when table doesn't exist."""
        from pyiceberg.exceptions import NoSuchTableError
        
        catalog = Mock()
        catalog.list_namespaces.return_value = [("test_db",)]
        catalog.load_table.side_effect = NoSuchTableError("Table not found")
        mock_load_catalog.return_value = catalog
        
        query_service = QueryService(integration_settings)
        
        # Should raise IcebergConnectionError
        from health_data_mcp.services.iceberg_client import IcebergConnectionError
        with pytest.raises(IcebergConnectionError):
            await query_service.get_daily_aggregates(
                user_id="user-123",
                data_type="heartRate"
            )
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    async def test_invalid_date_parameter(
        self, mock_load_catalog, integration_settings, mock_catalog_with_tables
    ):
        """Test error handling for invalid date format."""
        catalog, _, _ = mock_catalog_with_tables
        mock_load_catalog.return_value = catalog
        
        query_service = QueryService(integration_settings)
        
        with pytest.raises(ValidationError) as exc_info:
            await query_service.get_daily_aggregates(
                user_id="user-123",
                data_type="heartRate",
                start_date="2025/11/17",  # Invalid format
                end_date="2025-11-17"
            )
        
        assert "Invalid date format" in str(exc_info.value)
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    async def test_invalid_week_parameter(
        self, mock_load_catalog, integration_settings, mock_catalog_with_tables
    ):
        """Test error handling for invalid week format."""
        catalog, _, _ = mock_catalog_with_tables
        mock_load_catalog.return_value = catalog
        
        query_service = QueryService(integration_settings)
        
        with pytest.raises(ValidationError) as exc_info:
            await query_service.get_weekly_aggregates(
                user_id="user-123",
                data_type="steps",
                start_week="2025-45",  # Invalid format
                end_week="2025-W46"
            )
        
        assert "Invalid week format" in str(exc_info.value)

    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    async def test_invalid_month_parameter(
        self, mock_load_catalog, integration_settings, mock_catalog_with_tables
    ):
        """Test error handling for invalid month format."""
        catalog, _, _ = mock_catalog_with_tables
        mock_load_catalog.return_value = catalog
        
        query_service = QueryService(integration_settings)
        
        with pytest.raises(ValidationError) as exc_info:
            await query_service.get_monthly_aggregates(
                user_id="user-123",
                data_type="heartRate",
                start_month="2025/11",  # Invalid format
                end_month="2025-11"
            )
        
        assert "Invalid month format" in str(exc_info.value)
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    async def test_missing_required_parameter(
        self, mock_load_catalog, integration_settings, mock_catalog_with_tables
    ):
        """Test error handling for missing required parameters."""
        catalog, _, _ = mock_catalog_with_tables
        mock_load_catalog.return_value = catalog
        
        query_service = QueryService(integration_settings)
        
        with pytest.raises(ValidationError) as exc_info:
            await query_service.get_daily_aggregates(
                user_id="",  # Empty user_id
                data_type="heartRate"
            )
        
        assert "user_id" in str(exc_info.value)
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    async def test_invalid_date_range(
        self, mock_load_catalog, integration_settings, mock_catalog_with_tables
    ):
        """Test error handling when start_date > end_date."""
        catalog, _, _ = mock_catalog_with_tables
        mock_load_catalog.return_value = catalog
        
        query_service = QueryService(integration_settings)
        
        with pytest.raises(ValidationError) as exc_info:
            await query_service.get_daily_aggregates(
                user_id="user-123",
                data_type="heartRate",
                start_date="2025-11-30",
                end_date="2025-11-01"  # Before start_date
            )
        
        assert "Invalid date range" in str(exc_info.value)
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    async def test_invalid_sort_by_parameter(
        self, mock_load_catalog, integration_settings, mock_catalog_with_tables
    ):
        """Test error handling for invalid sort_by value."""
        catalog, _, _ = mock_catalog_with_tables
        mock_load_catalog.return_value = catalog
        
        query_service = QueryService(integration_settings)
        
        with pytest.raises(ValidationError) as exc_info:
            await query_service.get_top_records(
                user_id="user-123",
                data_type="steps",
                sort_by="invalid_field"
            )
        
        assert "Invalid sort_by value" in str(exc_info.value)
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    async def test_invalid_limit_parameter(
        self, mock_load_catalog, integration_settings, mock_catalog_with_tables
    ):
        """Test error handling for invalid limit value."""
        catalog, _, _ = mock_catalog_with_tables
        mock_load_catalog.return_value = catalog
        
        query_service = QueryService(integration_settings)
        
        with pytest.raises(ValidationError) as exc_info:
            await query_service.get_top_records(
                user_id="user-123",
                data_type="steps",
                limit=20000  # Exceeds max limit
            )
        
        assert "Invalid limit value" in str(exc_info.value)


@pytest.mark.integration
class TestMCPProtocolIntegration:
    """Integration tests for MCP protocol and tool calls."""
    
    @patch("health_data_mcp.main.query_service")
    async def test_mcp_get_daily_aggregates_tool(self, mock_query_service):
        """Test MCP tool: get_daily_aggregates."""
        # Mock the query service response
        mock_query_service.get_daily_aggregates.return_value = {
            "user_id": "user-123",
            "data_type": "heartRate",
            "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
            "records": [
                {
                    "aggregation_date": "2025-11-17",
                    "avg_value": 75.5
                }
            ],
            "count": 1
        }
        
        # Call the MCP tool directly
        result = await get_daily_aggregates(
            user_id="user-123",
            data_type="heartRate",
            start_date="2025-11-01",
            end_date="2025-11-30"
        )
        
        # Verify the tool was called correctly
        mock_query_service.get_daily_aggregates.assert_called_once_with(
            user_id="user-123",
            data_type="heartRate",
            start_date="2025-11-01",
            end_date="2025-11-30"
        )
        
        # Verify response structure
        assert "user_id" in result
        assert "data_type" in result
        assert "records" in result
        assert result["count"] == 1

    @patch("health_data_mcp.main.query_service")
    async def test_mcp_get_weekly_aggregates_tool(self, mock_query_service):
        """Test MCP tool: get_weekly_aggregates."""
        mock_query_service.get_weekly_aggregates.return_value = {
            "user_id": "user-123",
            "data_type": "steps",
            "week_range": {"start_week": "2025-W45", "end_week": "2025-W46"},
            "records": [
                {
                    "year": 2025,
                    "week_of_year": 46,
                    "avg_value": 10000.0
                }
            ],
            "count": 1
        }
        
        result = await get_weekly_aggregates(
            user_id="user-123",
            data_type="steps",
            start_week="2025-W45",
            end_week="2025-W46"
        )
        
        mock_query_service.get_weekly_aggregates.assert_called_once()
        assert result["count"] == 1
        assert "week_range" in result
    
    @patch("health_data_mcp.main.query_service")
    async def test_mcp_get_monthly_aggregates_tool(self, mock_query_service):
        """Test MCP tool: get_monthly_aggregates."""
        mock_query_service.get_monthly_aggregates.return_value = {
            "user_id": "user-123",
            "data_type": "heartRate",
            "month_range": {"start_month": "2025-10", "end_month": "2025-11"},
            "records": [
                {
                    "year": 2025,
                    "month": 11,
                    "avg_value": 75.1
                }
            ],
            "count": 1
        }
        
        result = await get_monthly_aggregates(
            user_id="user-123",
            data_type="heartRate",
            start_month="2025-10",
            end_month="2025-11"
        )
        
        mock_query_service.get_monthly_aggregates.assert_called_once()
        assert result["count"] == 1
        assert "month_range" in result
    
    @patch("health_data_mcp.main.query_service")
    async def test_mcp_get_top_records_tool(self, mock_query_service):
        """Test MCP tool: get_top_records."""
        mock_query_service.get_top_records.return_value = {
            "user_id": "user-123",
            "data_type": "steps",
            "date_range": {"start": "2025-11-01", "end": "2025-11-30"},
            "sort_by": "max_value",
            "order": "desc",
            "records": [
                {
                    "rank": 1,
                    "aggregation_date": "2025-11-15",
                    "value": 18000.0
                }
            ],
            "count": 1
        }
        
        result = await get_top_records(
            user_id="user-123",
            data_type="steps",
            start_date="2025-11-01",
            end_date="2025-11-30",
            sort_by="max_value",
            order="desc",
            limit=10
        )
        
        mock_query_service.get_top_records.assert_called_once()
        assert result["count"] == 1
        assert result["sort_by"] == "max_value"
        assert result["records"][0]["rank"] == 1
    
    @patch("health_data_mcp.main.query_service")
    async def test_mcp_tool_validation_error_handling(self, mock_query_service):
        """Test MCP tool error handling for validation errors."""
        # Mock validation error
        mock_query_service.get_daily_aggregates.side_effect = ValidationError(
            "Invalid date format",
            {"field": "start_date"}
        )
        
        result = await get_daily_aggregates(
            user_id="user-123",
            data_type="heartRate",
            start_date="invalid-date",
            end_date="2025-11-30"
        )
        
        # Verify error response structure
        assert "error" in result
        assert result["error"]["type"] == "ValidationError"
        assert "Invalid date format" in result["error"]["message"]
    
    @patch("health_data_mcp.main.query_service")
    async def test_mcp_tool_connection_error_handling(self, mock_query_service):
        """Test MCP tool error handling for connection errors."""
        from health_data_mcp.services.iceberg_client import IcebergConnectionError
        
        mock_query_service.get_daily_aggregates.side_effect = IcebergConnectionError(
            "Failed to connect to catalog"
        )
        
        result = await get_daily_aggregates(
            user_id="user-123",
            data_type="heartRate"
        )
        
        assert "error" in result
        assert result["error"]["type"] == "ConnectionError"
    
    @patch("health_data_mcp.main.query_service")
    async def test_mcp_tool_query_error_handling(self, mock_query_service):
        """Test MCP tool error handling for query errors."""
        from health_data_mcp.services.iceberg_client import IcebergQueryError
        
        mock_query_service.get_daily_aggregates.side_effect = IcebergQueryError(
            "Query execution failed"
        )
        
        result = await get_daily_aggregates(
            user_id="user-123",
            data_type="heartRate"
        )
        
        assert "error" in result
        assert result["error"]["type"] == "QueryError"
    
    @patch("health_data_mcp.main.query_service")
    async def test_mcp_tool_unexpected_error_handling(self, mock_query_service):
        """Test MCP tool error handling for unexpected errors."""
        mock_query_service.get_daily_aggregates.side_effect = Exception(
            "Unexpected error"
        )
        
        result = await get_daily_aggregates(
            user_id="user-123",
            data_type="heartRate"
        )
        
        assert "error" in result
        assert result["error"]["type"] == "InternalError"
