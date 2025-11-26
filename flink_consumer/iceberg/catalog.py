"""Iceberg REST Catalog management"""

import logging
from typing import Optional
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError

from flink_consumer.config.settings import Settings

logger = logging.getLogger(__name__)


class IcebergCatalog:
    """
    Iceberg REST Catalog wrapper for managing catalog connections
    
    Provides initialization, connection testing, and catalog operations
    for Apache Iceberg REST catalog.
    """

    def __init__(self, settings: Settings):
        """
        Initialize Iceberg catalog
        
        Args:
            settings: Application settings containing Iceberg configuration
        """
        self.settings = settings
        self._catalog: Optional[RestCatalog] = None
        self._catalog_config = self._build_catalog_config()

    def _build_catalog_config(self) -> dict:
        """
        Build catalog configuration dictionary
        
        Returns:
            Dictionary with catalog configuration
        """
        config = {
            "type": "rest",
            "uri": self.settings.iceberg.catalog_uri,
            "warehouse": self.settings.iceberg.warehouse,
            "s3.endpoint": self.settings.s3.endpoint,
            "s3.access-key-id": self.settings.s3.access_key,
            "s3.secret-access-key": self.settings.s3.secret_key,
            "s3.path-style-access": str(self.settings.s3.path_style_access).lower(),
        }
        
        logger.info(
            f"Built catalog config for {self.settings.iceberg.catalog_name}",
            extra={
                "catalog_type": "rest",
                "catalog_uri": self.settings.iceberg.catalog_uri,
                "warehouse": self.settings.iceberg.warehouse,
            }
        )
        
        return config

    def initialize(self) -> RestCatalog:
        """
        Initialize and connect to Iceberg REST catalog
        
        Returns:
            RestCatalog instance
            
        Raises:
            Exception: If catalog initialization fails
        """
        try:
            logger.info(
                f"Initializing Iceberg REST catalog: {self.settings.iceberg.catalog_name}"
            )
            
            self._catalog = load_catalog(
                self.settings.iceberg.catalog_name,
                **self._catalog_config
            )
            
            logger.info(
                f"Successfully initialized catalog: {self.settings.iceberg.catalog_name}"
            )
            
            return self._catalog
            
        except Exception as e:
            logger.error(
                f"Failed to initialize Iceberg catalog: {str(e)}",
                extra={
                    "catalog_name": self.settings.iceberg.catalog_name,
                    "catalog_uri": self.settings.iceberg.catalog_uri,
                    "error": str(e),
                },
                exc_info=True
            )
            raise

    def test_connection(self) -> bool:
        """
        Test catalog connection by listing namespaces
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            if self._catalog is None:
                self.initialize()
            
            # Test connection by listing namespaces
            namespaces = self._catalog.list_namespaces()
            
            logger.info(
                "Catalog connection test successful",
                extra={
                    "catalog_name": self.settings.iceberg.catalog_name,
                    "namespaces_count": len(namespaces),
                    "namespaces": [str(ns) for ns in namespaces],
                }
            )
            
            return True
            
        except Exception as e:
            logger.error(
                f"Catalog connection test failed: {str(e)}",
                extra={
                    "catalog_name": self.settings.iceberg.catalog_name,
                    "error": str(e),
                },
                exc_info=True
            )
            return False

    def create_namespace_if_not_exists(self, namespace: str) -> bool:
        """
        Create namespace (database) if it doesn't exist
        
        Args:
            namespace: Namespace name to create
            
        Returns:
            True if namespace was created or already exists
        """
        try:
            if self._catalog is None:
                self.initialize()
            
            # Check if namespace exists
            try:
                self._catalog.load_namespace_properties(namespace)
                logger.info(f"Namespace already exists: {namespace}")
                return True
            except NoSuchNamespaceError:
                # Namespace doesn't exist, create it
                self._catalog.create_namespace(namespace)
                logger.info(f"Created namespace: {namespace}")
                return True
                
        except Exception as e:
            logger.error(
                f"Failed to create namespace {namespace}: {str(e)}",
                extra={"namespace": namespace, "error": str(e)},
                exc_info=True
            )
            return False

    def table_exists(self, namespace: str, table_name: str) -> bool:
        """
        Check if a table exists in the catalog
        
        Args:
            namespace: Namespace (database) name
            table_name: Table name
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            if self._catalog is None:
                self.initialize()
            
            table_identifier = f"{namespace}.{table_name}"
            self._catalog.load_table(table_identifier)
            return True
            
        except NoSuchTableError:
            return False
        except Exception as e:
            logger.error(
                f"Error checking table existence: {str(e)}",
                extra={
                    "namespace": namespace,
                    "table_name": table_name,
                    "error": str(e),
                },
                exc_info=True
            )
            return False

    def get_catalog(self) -> RestCatalog:
        """
        Get the catalog instance, initializing if necessary
        
        Returns:
            RestCatalog instance
        """
        if self._catalog is None:
            self.initialize()
        return self._catalog

    def get_catalog_config(self) -> dict:
        """
        Get catalog configuration dictionary
        
        Returns:
            Catalog configuration
        """
        return self._catalog_config.copy()
