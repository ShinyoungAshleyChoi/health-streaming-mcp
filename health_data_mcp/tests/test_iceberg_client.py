"""Tests for Iceberg client."""

import pytest
from unittest.mock import Mock, MagicMock, patch
import pyarrow as pa

from health_data_mcp.services.iceberg_client import (
    IcebergClient,
    IcebergConnectionError,
    IcebergQueryError
)
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
def mock_catalog():
    """Create a mock Iceberg catalog."""
    catalog = Mock()
    catalog.list_namespaces.return_value = [("test_db",)]
    return catalog


class TestIcebergClient:
    """Tests for IcebergClient class."""
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    def test_init_success(self, mock_load_catalog, mock_settings, mock_catalog):
        """Test successful IcebergClient initialization."""
        mock_load_catalog.return_value = mock_catalog
        
        client = IcebergClient(mock_settings)
        
        assert client.settings == mock_settings
        assert client.catalog == mock_catalog
        mock_load_catalog.assert_called_once()
        mock_catalog.list_namespaces.assert_called_once()
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    def test_init_catalog_connection_failure(self, mock_load_catalog, mock_settings):
        """Test IcebergClient initialization fails when catalog connection fails."""
        mock_load_catalog.side_effect = Exception("Connection failed")
        
        with pytest.raises(IcebergConnectionError) as exc_info:
            IcebergClient(mock_settings)
        
        assert "Failed to initialize Iceberg catalog" in str(exc_info.value)
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    def test_init_database_not_found(self, mock_load_catalog, mock_settings):
        """Test IcebergClient initialization fails when database doesn't exist."""
        mock_catalog = Mock()
        mock_catalog.list_namespaces.return_value = [("other_db",)]
        mock_load_catalog.return_value = mock_catalog
        
        with pytest.raises(IcebergConnectionError) as exc_info:
            IcebergClient(mock_settings)
        
        assert "Database 'test_db' not found" in str(exc_info.value)
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    def test_load_table_success(self, mock_load_catalog, mock_settings, mock_catalog):
        """Test successful table loading."""
        mock_load_catalog.return_value = mock_catalog
        mock_table = Mock()
        mock_catalog.load_table.return_value = mock_table
        
        client = IcebergClient(mock_settings)
        table = client.load_table("test_table")
        
        assert table == mock_table
        mock_catalog.load_table.assert_called_with("test_db.test_table")
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    def test_load_table_with_full_name(self, mock_load_catalog, mock_settings, mock_catalog):
        """Test table loading with full table name (database.table)."""
        mock_load_catalog.return_value = mock_catalog
        mock_table = Mock()
        mock_catalog.load_table.return_value = mock_table
        
        client = IcebergClient(mock_settings)
        table = client.load_table("test_db.test_table")
        
        assert table == mock_table
        mock_catalog.load_table.assert_called_with("test_db.test_table")
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    def test_load_table_not_found(self, mock_load_catalog, mock_settings, mock_catalog):
        """Test table loading fails when table doesn't exist."""
        from pyiceberg.exceptions import NoSuchTableError
        
        mock_load_catalog.return_value = mock_catalog
        mock_catalog.load_table.side_effect = NoSuchTableError("Table not found")
        
        client = IcebergClient(mock_settings)
        
        with pytest.raises(IcebergConnectionError) as exc_info:
            client.load_table("nonexistent_table")
        
        assert "Table 'nonexistent_table' not found" in str(exc_info.value)
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    def test_scan_table_success(self, mock_load_catalog, mock_settings, mock_catalog):
        """Test successful table scanning."""
        mock_load_catalog.return_value = mock_catalog
        
        # Create mock table and scan
        mock_table = Mock()
        mock_scan = Mock()
        mock_table.scan.return_value = mock_scan
        mock_scan.limit.return_value = mock_scan
        
        # Create mock PyArrow table
        mock_arrow_table = pa.table({
            "user_id": ["user-1", "user-2"],
            "data_type": ["heartRate", "heartRate"],
            "avg_value": [75.0, 80.0]
        })
        mock_scan.to_arrow.return_value = mock_arrow_table
        
        mock_catalog.load_table.return_value = mock_table
        
        client = IcebergClient(mock_settings)
        results = client.scan_table("test_table", limit=10)
        
        assert len(results) == 2
        assert results[0]["user_id"] == "user-1"
        assert results[1]["user_id"] == "user-2"
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    def test_scan_table_with_filters(self, mock_load_catalog, mock_settings, mock_catalog):
        """Test table scanning with filters."""
        mock_load_catalog.return_value = mock_catalog
        
        # Create mock table and scan
        mock_table = Mock()
        mock_scan = Mock()
        mock_table.scan.return_value = mock_scan
        mock_scan.filter.return_value = mock_scan
        mock_scan.limit.return_value = mock_scan
        
        # Create mock PyArrow table
        mock_arrow_table = pa.table({
            "user_id": ["user-1"],
            "data_type": ["heartRate"],
            "avg_value": [75.0]
        })
        mock_scan.to_arrow.return_value = mock_arrow_table
        
        mock_catalog.load_table.return_value = mock_table
        
        client = IcebergClient(mock_settings)
        filters = [
            ("user_id", "==", "user-1"),
            ("avg_value", ">=", 70.0)
        ]
        results = client.scan_table("test_table", filters=filters)
        
        assert len(results) == 1
        assert results[0]["user_id"] == "user-1"
        # Verify filter was called
        assert mock_scan.filter.call_count == 2
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    def test_scan_table_with_selected_fields(self, mock_load_catalog, mock_settings, mock_catalog):
        """Test table scanning with field selection."""
        mock_load_catalog.return_value = mock_catalog
        
        # Create mock table and scan
        mock_table = Mock()
        mock_scan = Mock()
        mock_table.scan.return_value = mock_scan
        mock_scan.select.return_value = mock_scan
        mock_scan.limit.return_value = mock_scan
        
        # Create mock PyArrow table
        mock_arrow_table = pa.table({
            "user_id": ["user-1"],
            "avg_value": [75.0]
        })
        mock_scan.to_arrow.return_value = mock_arrow_table
        
        mock_catalog.load_table.return_value = mock_table
        
        client = IcebergClient(mock_settings)
        results = client.scan_table(
            "test_table",
            selected_fields=["user_id", "avg_value"]
        )
        
        assert len(results) == 1
        mock_scan.select.assert_called_once_with("user_id", "avg_value")
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    def test_scan_table_query_error(self, mock_load_catalog, mock_settings, mock_catalog):
        """Test table scanning fails with query error."""
        mock_load_catalog.return_value = mock_catalog
        
        mock_table = Mock()
        mock_scan = Mock()
        mock_table.scan.return_value = mock_scan
        mock_scan.limit.return_value = mock_scan
        mock_scan.to_arrow.side_effect = Exception("Query failed")
        
        mock_catalog.load_table.return_value = mock_table
        
        client = IcebergClient(mock_settings)
        
        with pytest.raises(IcebergQueryError) as exc_info:
            client.scan_table("test_table")
        
        assert "Failed to scan table" in str(exc_info.value)
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    def test_apply_filters_unsupported_operator(self, mock_load_catalog, mock_settings, mock_catalog):
        """Test _apply_filters raises error for unsupported operator."""
        mock_load_catalog.return_value = mock_catalog
        
        mock_table = Mock()
        mock_scan = Mock()
        mock_table.scan.return_value = mock_scan
        
        mock_catalog.load_table.return_value = mock_table
        
        client = IcebergClient(mock_settings)
        
        with pytest.raises(IcebergQueryError) as exc_info:
            filters = [("user_id", "INVALID", "user-1")]
            client.scan_table("test_table", filters=filters)
        
        assert "Unsupported operator" in str(exc_info.value)
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    def test_list_tables_success(self, mock_load_catalog, mock_settings, mock_catalog):
        """Test successful table listing."""
        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_tables.return_value = [
            ("test_db", "table1"),
            ("test_db", "table2")
        ]
        
        client = IcebergClient(mock_settings)
        tables = client.list_tables()
        
        assert len(tables) == 2
        assert "test_db.table1" in tables
        assert "test_db.table2" in tables
    
    @patch("health_data_mcp.services.iceberg_client.load_catalog")
    def test_list_tables_failure(self, mock_load_catalog, mock_settings, mock_catalog):
        """Test table listing fails with connection error."""
        mock_load_catalog.return_value = mock_catalog
        mock_catalog.list_tables.side_effect = Exception("Failed to list tables")
        
        client = IcebergClient(mock_settings)
        
        with pytest.raises(IcebergConnectionError) as exc_info:
            client.list_tables()
        
        assert "Failed to list tables" in str(exc_info.value)
