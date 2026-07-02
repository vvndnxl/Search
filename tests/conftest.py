import os
import csv
import hashlib
import asyncio
import pytest
import pytest_asyncio
import httpx
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import NullPool

# Force testing database environments before any internal application package executes
os.environ["POSTGRES_DB"] = "test_search_db"

from app.config import settings
from app.database import Base, get_db_session, get_elastic_ctx, get_kafka_ctx, get_redis_ctx
from app.models import DocumentModel
from app.main import app

TEST_DATABASE_URL = settings.DATABASE_URL
test_engine = create_async_engine(TEST_DATABASE_URL, echo=False, poolclass=NullPool)
TestAsyncSessionLocal = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)

TEST_INDEX = f"test_{settings.ELASTIC_INDEX}".lower().strip()


@pytest.fixture(scope="session")
def event_loop():
    """Enforces a single session-wide async event loop to eliminate all different loop panics."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_test_db(event_loop):
    """Initializes isolated relation table architectures once per suite execution pass."""
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await test_engine.dispose()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_test_elasticsearch(event_loop):
    """Initializes and deletes temporary elastic index topologies cleanly."""
    from app.database import create_elastic_client
    from elasticsearch import NotFoundError
    es_client = create_elastic_client()
    try:
        await es_client.indices.delete(index=TEST_INDEX)
    except NotFoundError:
        pass
    await es_client.indices.create(
        index=TEST_INDEX,
        body={"mappings": {"properties": {"text": {"type": "text", "analyzer": "russian"}}}}
    )
    yield
    try:
        await es_client.indices.delete(index=TEST_INDEX)
    except NotFoundError:
        pass
    await es_client.close()


@pytest_asyncio.fixture
async def db_session():
    """Provides a loop-isolated relational session token per test block execution pass."""
    async with TestAsyncSessionLocal() as session:
        yield session
        await session.close()


class MockKafkaProducer:
    def __init__(self):
        self.sent_messages = []

    async def send_and_wait(self, topic, key, value):
        self.sent_messages.append({"topic": topic, "key": key, "value": value})


@pytest.fixture
def mock_kafka():
    return MockKafkaProducer()


@pytest_asyncio.fixture
async def client(mock_kafka):
    """
    Maintains a pristine ASGI client wrapper ensuring that both test suites
    and internal endpoint scopes communicate through the identical loop-safe connection tokens.
    """
    from app.database import create_elastic_client
    from redis.asyncio import Redis, ConnectionPool
    es_client = create_elastic_client()

    test_redis_pool = ConnectionPool(
        host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB, decode_responses=True
    )
    test_redis_client = Redis(connection_pool=test_redis_pool)

    try:
        await test_redis_client.flushdb()
    except Exception:
        pass

    async def _get_test_db():
        async with TestAsyncSessionLocal() as session:
            yield session

    async def _get_test_elastic():
        yield es_client

    async def _get_test_kafka():
        yield mock_kafka

    async def _get_test_redis():
        yield test_redis_client

    # Apply precise, explicit context dependency overruns onto the application module instance
    app.dependency_overrides[get_db_session] = _get_test_db
    app.dependency_overrides[get_elastic_ctx] = _get_test_elastic
    app.dependency_overrides[get_kafka_ctx] = _get_test_kafka
    app.dependency_overrides[get_redis_ctx] = _get_test_redis

    old_index = settings.ELASTIC_INDEX
    settings.ELASTIC_INDEX = TEST_INDEX

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as async_http_client:
        yield async_http_client

    # Clean up and reset dependencies, restoring original production boundaries safely
    app.dependency_overrides.clear()
    settings.ELASTIC_INDEX = old_index

    try:
        await test_redis_client.flushdb()
        await test_redis_client.aclose()
        await test_redis_pool.disconnect()
    except Exception:
        pass
    await es_client.close()


@pytest.fixture(scope="session")
def raw_csv_documents():
    """
    Парсит записи из data.csv, генерируя гарантированно уникальные
    последовательные ID, полностью исключая ошибки UniqueViolationError.
    """
    csv_path = "/app/data.csv"
    processed_records = []
    if not os.path.exists(csv_path):
        return processed_records

    with open(csv_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for idx, row in enumerate(reader):
            try:
                rubrics_list = eval(row["rubrics"]) if row["rubrics"].startswith("[") else [row["rubrics"]]
                created_dt = datetime.strptime(row["created_date"], "%Y-%m-%d %H:%M:%S")

                # Возвращаем абсолютно надежный инкремент, защищенный от дубликатов текста в CSV
                doc_id = idx + 1

                processed_records.append({
                    "id": doc_id,
                    "rubrics": rubrics_list,
                    "text": row["text"],
                    "created_date": created_dt
                })
            except Exception:
                continue
    return processed_records

