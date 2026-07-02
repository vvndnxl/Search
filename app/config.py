import os
from pydantic_settings import BaseSettings, SettingsConfigDict


# =====================================================================
# 1. CONFIGURATION & SETTINGS
# =====================================================================
import os
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # App General Meta Properties (Required by app/main.py metadata blocks)
    PROJECT_NAME: str = "Document Search Engine"

    # Relational Database Components (Pure atomic properties to avoid property setter blocks)
    POSTGRES_HOST: str = "search_postgres"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_DB: str = "search_db"

    # Elastic Host Infrastructure Config (Split host and ports to satisfy create_elastic_client)
    ELASTIC_HOST: str = "search_elastic"
    ELASTIC_PORT: int = 9200
    ELASTIC_INDEX: str = "documents"

    # Synchronized Streaming Platform Elements
    KAFKA_BOOTSTRAP_SERVERS: str = "search_kafka:9092"
    KAFKA_TOPIC: str = "document_sync"

    # Redis Distributed Caching Architecture Variables
    REDIS_HOST: str = "search_redis"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    CACHE_TTL_SECONDS: int = 300

    @property
    def DATABASE_URL(self) -> str:
        """Assembles database connections using native Pydantic atomic overrides."""
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    ELASTIC_SEARCH_LIMIT: int = 500

    # Kafka settings
    KAFKA_GROUP_ID: str = "elastic-cleaner-group"

    # Business limits
    DB_DISPLAY_LIMIT: int = 20

    # FIX: Clean Pydantic V2 BaseSettings Configuration Dictionary
    model_config = SettingsConfigDict(
        case_sensitive=True,
        env_file=".env",
        extra='ignore'
    )


settings = Settings()
