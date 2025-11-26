"""Shared test fixtures and configuration."""

import pytest
import os


@pytest.fixture(autouse=True)
def reset_env_vars(monkeypatch):
    """Reset environment variables before each test."""
    # Clear any existing environment variables that might interfere with tests
    env_vars_to_clear = [
        "ICEBERG_CATALOG_NAME",
        "ICEBERG_CATALOG_URI",
        "ICEBERG_WAREHOUSE",
        "ICEBERG_DATABASE",
        "S3_ENDPOINT",
        "S3_ACCESS_KEY",
        "S3_SECRET_KEY",
        "TABLE_DAILY_AGG",
        "TABLE_WEEKLY_AGG",
        "TABLE_MONTHLY_AGG",
        "DEFAULT_DAILY_RANGE_DAYS",
        "DEFAULT_WEEKLY_RANGE_WEEKS",
        "DEFAULT_MONTHLY_RANGE_MONTHS",
    ]
    
    for var in env_vars_to_clear:
        monkeypatch.delenv(var, raising=False)
