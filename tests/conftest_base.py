import asyncio
import os

import pytest
import pytest_asyncio
import httpx
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import NullPool

from app.config import settings
from app.database import Base, get_db_session, create_elastic_client
from app.main import app, get_elastic_ctx, get_kafka_ctx

# Force a session-isolated testing database string
TEST_DATABASE_URL = settings.DATABASE_URL.replace(settings.POSTGRES_DB, "test_search_db")

# Crucial: poolclass=None (NullPool) ensures that connections are dropped and recreated
# instead of stored in a connection pool that caches old event loop states!

os.environ["POSTGRES_DB"] = "test_search_db"

test_engine = create_async_engine(TEST_DATABASE_URL, echo=False, poolclass=NullPool)
TestAsyncSessionLocal = async_sessionmaker(bind=test_engine, class_=AsyncSession, expire_on_commit=False)

TEST_INDEX = f"test_{settings.ELASTIC_INDEX}".lower().strip()


# =====================================================================
# 1. FIXED EVENT LOOP SCOPING
# =====================================================================
@pytest.fixture(scope="session")
def event_loop():
    """Forces a single, shared event loop across all tests to prevent loop mismatches."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_test_db(event_loop):
    """Initializes schema tables once per test suite lifecycle."""
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await test_engine.dispose()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_test_elasticsearch():
    """Maintains a dedicated test index footprint inside Elasticsearch."""
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


# =====================================================================
# 2. ISOLATED CONTEXT COUPLING
# =====================================================================
@pytest_asyncio.fixture
async def db_session():
    """Provides a perfectly clean, loop-isolated session for the test logic."""
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
    Creates an ASGI transport client where the application and the tests
    instantiate fresh connections dynamically from the same event loop.
    """
    es_client = create_elastic_client()

    # Create distinct session factories to avoid cross-request contamination
    async def _get_test_db():
        async with TestAsyncSessionLocal() as session:
            yield session

    async def _get_test_elastic():
        yield es_client

    async def _get_test_kafka():
        yield mock_kafka

    # Assign runtime dependencies safely
    app.dependency_overrides[get_db_session] = _get_test_db
    app.dependency_overrides[get_elastic_ctx] = _get_test_elastic
    app.dependency_overrides[get_kafka_ctx] = _get_test_kafka

    old_index = settings.ELASTIC_INDEX
    settings.ELASTIC_INDEX = TEST_INDEX

    # Use native ASGI transport mapping to run directly on the active test thread loop
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as async_http_client:
        yield async_http_client

    app.dependency_overrides.clear()
    settings.ELASTIC_INDEX = old_index
    await es_client.close()
