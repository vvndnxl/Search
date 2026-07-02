import os
import csv
import asyncio
import pytest
import pytest_asyncio
import httpx
from datetime import datetime

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import NullPool

from app.config import settings
from app.database import Base, get_db_session, create_elastic_client, get_redis_ctx
from app.models import DocumentModel
from app.main import app, get_elastic_ctx, get_kafka_ctx

# Inject atomic properties via os.environ before importing settings
# This ensures Pydantic reads the test database properties during initialization
os.environ["POSTGRES_DB"] = "test_search_db"


# The property reads the updated environment fields automatically without a setter crash
TEST_DATABASE_URL = settings.DATABASE_URL

test_engine = create_async_engine(TEST_DATABASE_URL, echo=False, poolclass=NullPool)
TestAsyncSessionLocal = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)

TEST_INDEX = f"test_{settings.ELASTIC_INDEX}".lower().strip()


@pytest.fixture(scope="session")
def event_loop():
    """Guarantees a single, unified async event loop across all tests to prevent cross-talk blocks."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def raw_csv_documents():
    """Parses real testing records straight from the mounted workspace root path."""
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

                processed_records.append({
                    "id": idx + 1,
                    "rubrics": rubrics_list,
                    "text": row["text"],
                    "created_date": created_dt
                })
            except Exception:
                continue
    return processed_records


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_test_db(event_loop):
    """Rebuilds target relational tables once before the testing pipeline kicks off."""
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await test_engine.dispose()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_test_elasticsearch():
    """Creates a temporary, dedicated elastic index configured with explicit Russian morphology."""
    es_client = create_elastic_client()
    from elasticsearch import NotFoundError
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


# 3. Test Doubles and Framework Context Interceptors
class MockKafkaProducer:
    def __init__(self):
        self.sent_messages = []

    async def send_and_wait(self, topic, key, value):
        self.sent_messages.append({"topic": topic, "key": key, "value": value})


@pytest.fixture
def mock_kafka():
    return MockKafkaProducer()


# =====================================================================
# REQUEST-SCOPED CONTEXT MOCKS (STAGE 1: ISOLATED ASYNC LIFECYCLE)
# =====================================================================

@pytest_asyncio.fixture
async def client(mock_kafka, raw_csv_documents):
    """
    Interceptors app contexts, binding them to isolated mock dependencies dynamically.
    Ensures that the Redis connection pool binds strictly to the active asynchronous
    event loop of the executing test case to prevent cross-loop socket errors.
    """
    es_client = create_elastic_client()

    # Establish a fresh connection pool explicitly bound to the current running event loop
    from redis.asyncio import Redis, ConnectionPool
    test_redis_pool = ConnectionPool(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        decode_responses=True
    )
    test_redis_client = Redis(connection_pool=test_redis_pool)

    # Safely clear out any keys inside the volatile database slice before execution
    try:
        await test_redis_client.flushdb()
    except Exception:
        pass  # Gracefully fall back if Redis container hasn't completed bootstrap initialization

    async def _get_test_db():
        async with TestAsyncSessionLocal() as session:
            yield session

    async def _get_test_elastic():
        yield es_client

    async def _get_test_kafka():
        yield mock_kafka

    async def _get_test_redis():
        yield test_redis_client

    # Inject our loop-aligned test doubles directly into the running application context
    app.dependency_overrides[get_db_session] = _get_test_db
    app.dependency_overrides[get_elastic_ctx] = _get_test_elastic
    app.dependency_overrides[get_kafka_ctx] = _get_test_kafka
    app.dependency_overrides[get_redis_ctx] = _get_test_redis

    old_index = settings.ELASTIC_INDEX
    settings.ELASTIC_INDEX = TEST_INDEX

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as async_http_client:
        yield async_http_client

    # Clean up and reset dependencies, utilizing modern aclose() to drop connections cleanly
    app.dependency_overrides.clear()
    settings.ELASTIC_INDEX = old_index

    try:
        await test_redis_client.flushdb()
        # FIX: Replaced legacy .close() with modern .aclose() to clear deprecation tracking warnings
        await test_redis_client.aclose()
        await test_redis_pool.disconnect()
    except Exception:
        pass

    await es_client.close()

