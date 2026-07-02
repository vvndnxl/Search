import logging
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from elasticsearch import AsyncElasticsearch
from aiokafka import AIOKafkaProducer
from app.config import settings
from redis.asyncio import Redis, ConnectionPool
from app.config import settings

logger = logging.getLogger("app.database")

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_pre_ping=True
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

redis_pool = ConnectionPool(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
    decode_responses=True
)

Base = declarative_base()


def create_elastic_client() -> AsyncElasticsearch:
    """
    Creates an optimized, resilient modern asynchronous Elasticsearch 9.x client.
    Configured with a fail-fast request timeout to prevent FastAPI threads
    from locking up indefinitely during unexpected cluster latency spikes.
    """
    return AsyncElasticsearch(
        hosts=[f"http://{settings.ELASTIC_HOST}:{settings.ELASTIC_PORT}"],
        meta_header=True,
        request_timeout=5.0,  # Guardrail: Fail fast after 5s instead of blocking threads
        max_retries=3,        # Automatic recovery for transient dropped packets
        retry_on_timeout=True
    )


async def get_redis_ctx() -> AsyncGenerator[Redis, None]:
    """
    Dynamically initializes a loop-bound Redis connection pool.
    Enforces maximum connection limits to protect memory footprints
    and avoid connection leakage under high-concurrency conditions.
    """
    pool = ConnectionPool(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        decode_responses=True,
        max_connections=50,       # Guardrail: Cap maximum connections per worker task loop
        socket_timeout=3.0,       # Network fail-fast threshold
        socket_connect_timeout=3.0
    )
    client = Redis(connection_pool=pool)
    try:
        yield client
    except Exception as e:
        logger.error(f"⚠️ [DATABASE] Unexpected exception trapped inside Redis lifecycle hook: {e}")
        raise
    finally:
        # Guarantee that sockets are released back to the OS descriptor cache under all circumstances
        await client.aclose()
        await pool.disconnect()


async def get_db_session():
    """FastAPI Request Dependency yielding scoped DB connections."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


def create_kafka_producer() -> AIOKafkaProducer:
    """Returns a standalone, unmanaged Kafka producer instance tied to the active loop."""
    return AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)


# app/database.py

# ... (engine, AsyncSessionLocal, redis_pool, Base, create_elastic_client, create_kafka_producer остаются без изменений) ...

_elastic_client: AsyncElasticsearch | None = None
_kafka_producer: AIOKafkaProducer | None = None


async def init_infrastructure():
    """
    Fail-Fast инициализация синглтонов.
    Вызывается строго один раз при старте приложения внутри lifespan.
    """
    global _elastic_client, _kafka_producer

    # 1. Инициализируем глобальный Elasticsearch клиент
    if _elastic_client is None:
        _elastic_client = create_elastic_client()

    # Принудительно настраиваем маппинги и анализаторы до запуска API
    await verify_and_setup_elastic_index(_elastic_client)

    # 2. Инициализируем и прогреваем глобальный синглтон Kafka Producer
    if _kafka_producer is None:
        _kafka_producer = create_kafka_producer()
        await _kafka_producer.start()
        print("[Database] Infrastructure singletons initialized and pre-warmed.")


async def close_infrastructure():
    """Безопасное освобождение дескрипторов при завершении работы."""
    global _elastic_client, _kafka_producer
    print("[Database] Closing infrastructure connections...")

    if _kafka_producer is not None:
        await _kafka_producer.stop()
        _kafka_producer = None

    if _elastic_client is not None:
        await _elastic_client.close()
        _elastic_client = None


async def get_elastic_client() -> AsyncElasticsearch:
    """Прямой доступ к синглтону для ETL и тестов."""
    global _elastic_client
    if _elastic_client is None:
        _elastic_client = create_elastic_client()
    return _elastic_client


async def get_kafka_producer() -> AIOKafkaProducer:
    """Прямой доступ к синглтону."""
    global _kafka_producer
    if _kafka_producer is None:
        _kafka_producer = create_kafka_producer()
    return _kafka_producer


# --- Переписанные контексты-генераторы (Заменяют старые scoped-версии) ---

async def get_elastic_ctx() -> AsyncGenerator[AsyncElasticsearch, None]:
    """Отдает готовый синглтон без постоянного пересоздания клиента."""
    yield await get_elastic_client()


async def get_kafka_ctx() -> AsyncGenerator[AsyncElasticsearch, None]:
    """Отдает готовый прогретый синглтон из lifespan (без утечек дескрипторов)."""
    yield await get_kafka_producer()


async def verify_and_setup_elastic_index(es_client: AsyncElasticsearch):
    """
    Обеспечивает строгое наличие поискового индекса с русским анализатором.
    Если создание падает — падает все приложение (Fail-Fast).
    """
    index_name = settings.ELASTIC_INDEX

    exists = await es_client.indices.exists(index=index_name)
    if not exists:
        print(f"[Elastic] Index '{index_name}' missing. Building morphologic schema mappings...")
        index_body = {
            "settings": {
                "analysis": {
                    "analyzer": {
                        "default": {
                            "type": "russian"
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "text": {
                        "type": "text",
                        "analyzer": "russian"
                    }
                }
            }
        }
        await es_client.indices.create(index=index_name, body=index_body)
        print(f"[Elastic] Index '{index_name}' successfully provisioned with Russian analyzer.")
    else:
        settings_res = await es_client.indices.get_settings(index=index_name)
        if "russian" not in str(settings_res):
            raise RuntimeError(
                f"Critical Error: Elasticsearch index '{index_name}' exists "
                f"but is missing the required 'russian' analyzer configuration!"
            )
