"""
Configuration management using pydantic-settings
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings"""
    
    # API Settings
    app_name: str = "Health Stack Kafka Gateway"
    app_version: str = "1.0.0"
    api_prefix: str = "/api/v1"
    
    # Server Settings
    host: str = "0.0.0.0"
    port: int = 3000
    
    # SSL Settings
    ssl_enabled: bool = False
    ssl_certfile: str = "./certs/cert.pem"
    ssl_keyfile: str = "./certs/key.pem"
    
    # Kafka Settings
    kafka_brokers: str = "0.0.0.0:19092,0.0.0.0:19093,0.0.0.0:19094"
    kafka_topic: str = "health-data-raw"
    kafka_dlq_topic: str = "health-data-dlq"
    kafka_client_id: str = "health-stack-gateway"
    
    # Schema Registry Settings
    schema_registry_url: str = "http://0.0.0.0:8081"
    
    # Logging
    log_level: str = "INFO"
    
    # Schema Settings
    schema_path: str = "./schemas/health-data.avsc"
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False
    )


settings = Settings()
