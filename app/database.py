from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from elasticsearch import AsyncElasticsearch
from aiokafka import AIOKafkaProducer
from app.config import settings
from redis.asyncio import Redis, ConnectionPool
from app.config import settings




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
    decode_responses=True  # Automatically processes network streams into UTF-8 strings
)

Base = declarative_base()


async def get_redis_ctx() -> Redis:
    """
    Yields an active redis client connection token from the shared state pool.
    Designed for clean resource injection across standard FastAPI dependency layers.
    """
    client = Redis(connection_pool=redis_pool)
    try:
        yield client
    finally:
        # Closes the client interface tracking session without dropping the global pool
        await client.close()


async def get_db_session():
    """FastAPI Request Dependency yielding scoped DB connections."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


def create_elastic_client() -> AsyncElasticsearch:
    """
    Creates a clean, native, modern asynchronous Elasticsearch 9.x client.
    No more legacy header workarounds, reflection patches, or node pool mutations.
    """
    return AsyncElasticsearch(
        hosts=[f"http://{settings.ELASTIC_HOST}:{settings.ELASTIC_PORT}"],
        meta_header=True  # Standard modern library telemetry
    )


def create_kafka_producer() -> AIOKafkaProducer:
    """Returns a standalone, unmanaged Kafka producer instance tied to the active loop."""
    return AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)


async def verify_and_setup_elastic_index(es_client: AsyncElasticsearch):
    """Ensures index configuration with custom morphological text filters exists."""
    index_exists = await es_client.indices.exists(index=settings.ELASTIC_INDEX)
    if not index_exists:
        fallback_mapping = {
            "mappings": {
                "properties": {
                    "text": {"type": "text", "analyzer": "russian"}
                }
            }
        }
        await es_client.indices.create(index=settings.ELASTIC_INDEX, body=fallback_mapping)


# =====================================================================
# 3. ELASTICSEARCH & KAFKA CLIENTS (Singletons & Dependencies)
# =====================================================================
_elastic_client: AsyncElasticsearch | None = None
_kafka_producer: AIOKafkaProducer | None = None


async def init_infrastructure():
    """Инициализация глобальных клиентов и подготовка индексов при старте приложения."""
    global _elastic_client, _kafka_producer

    # Инициализация Elasticsearch
    _elastic_client = AsyncElasticsearch(hosts=[f"http://{settings.ELASTIC_HOST}:{settings.ELASTIC_PORT}"])

    # Проверка и создание индекса с поддержкой русской морфологии
    index_exists = await _elastic_client.indices.exists(index=settings.ELASTIC_INDEX)
    if not index_exists:
        index_mapping = {
            "settings": {
                "analysis": {
                    "analyzer": {
                        "russian_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "russian_morphology", "stop"]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "text": {
                        "type": "text",
                        "analyzer": "russian_analyzer"
                    }
                }
            }
        }
        # Если плагин russian_morphology не установлен в инстансе Elastic,
        # можно использовать встроенный тип "russian":
        # "analyzer": "russian" вместо кастомного russian_analyzer.
        try:
            await _elastic_client.indices.create(index=settings.ELASTIC_INDEX, body=index_mapping)
        except Exception:
            # Фолбэк на дефолтный русский анализатор, если плагин морфологии отсутствует
            fallback_mapping = {
                "mappings": {
                    "properties": {
                        "text": {"type": "text", "analyzer": "russian"}
                    }
                }
            }
            await _elastic_client.indices.create(index=settings.ELASTIC_INDEX, body=fallback_mapping)

    # Инициализация Kafka Producer
    _kafka_producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
    await _kafka_producer.start()


async def close_infrastructure():
    """Корректное закрытие соединений при остановке приложения."""
    global _elastic_client, _kafka_producer
    if _kafka_producer:
        await _kafka_producer.stop()
    if _elastic_client:
        await _elastic_client.close()


async def get_elastic_client() -> AsyncElasticsearch:
    """FastAPI Dependency for Elasticsearch."""
    if _elastic_client is None:
        raise RuntimeError("Elasticsearch client is not initialized.")
    return _elastic_client


async def get_kafka_producer() -> AIOKafkaProducer:
    """FastAPI Dependency for Kafka Producer."""
    if _kafka_producer is None:
        raise RuntimeError("Kafka producer is not initialized.")
    return _kafka_producer
