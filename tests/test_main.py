import json
import pytest
from datetime import datetime
from sqlalchemy import text

from app.models import DocumentModel
from app.database import create_elastic_client
from app.config import settings
from tests.conftest import TestAsyncSessionLocal


@pytest.mark.asyncio
async def test_production_csv_search_flow_sorting_and_limits(client, raw_csv_documents):
    """
    GIVEN a real dataset parsed dynamically from the production data.csv file
    WHEN a morphology-based search request is submitted to the API engine
    THEN it must return up to 20 documents, sorted strictly newest-first.
    """
    assert len(raw_csv_documents) > 0, "Error: data.csv is empty or path resolution failed!"

    es_client = create_elastic_client()
    test_index = settings.ELASTIC_INDEX

    # FIX: Dynamically extract table name from the model layer to avoid "relation 'docs' does not exist"
    table_name = DocumentModel.__tablename__

    # Clean out any old records using the true table name mapping
    async with TestAsyncSessionLocal() as session:
        await session.execute(text(f"TRUNCATE TABLE {table_name} CASCADE;"))
        await session.commit()
    await es_client.delete_by_query(index=test_index, body={"query": {"match_all": {}}})

    # Seed PostgreSQL and Elasticsearch concurrently with production row values
    async with TestAsyncSessionLocal() as session:
        for doc in raw_csv_documents:
            db_doc = DocumentModel(
                id=doc["id"],
                rubrics=doc["rubrics"],
                text=doc["text"],
                created_date=doc["created_date"]
            )
            session.add(db_doc)

            await es_client.index(
                index=test_index,
                id=str(doc["id"]),
                body={"text": doc["text"]}
            )
        await session.commit()

    await es_client.indices.refresh(index=test_index)

    # Pick a common target search query present within the data.csv
    target_query = "документ"
    response = await client.get(f"/api/v1/search?query={target_query}")

    assert response.status_code == 200
    results = response.json()

    assert len(results) <= 20, f"Specification Breach: API returned {len(results)} items, exceeding limit of 20!"

    if len(results) > 1:
        for i in range(len(results) - 1):
            current_date = datetime.fromisoformat(results[i]["created_date"])
            next_date = datetime.fromisoformat(results[i + 1]["created_date"])
            assert current_date >= next_date, (
                f"Sorting Failure at index {i}: "
                f"Document {results[i]['id']} ({current_date}) is older than "
                f"Document {results[i + 1]['id']} ({next_date})!"
            )

    await es_client.close()


@pytest.mark.asyncio
async def test_production_document_deletion_cascades(client, raw_csv_documents, mock_kafka):
    """
    GIVEN a live synchronized document row originating from data.csv
    WHEN a DELETE request targeting its unique ID is dispatched to the API
    THEN it must drop immediately from Postgres and append an entry to Kafka.
    """
    assert len(raw_csv_documents) > 0
    table_name = DocumentModel.__tablename__

    async with TestAsyncSessionLocal() as session:
        await session.execute(text(f"TRUNCATE TABLE {table_name} CASCADE;"))
        await session.commit()

    target_data = raw_csv_documents[0]
    target_id = target_data["id"]

    async with TestAsyncSessionLocal() as session:
        db_doc = DocumentModel(
            id=target_id,
            rubrics=target_data["rubrics"],
            text=target_data["text"],
            created_date=target_data["created_date"]
        )
        session.add(db_doc)
        await session.commit()

    response = await client.delete(f"/api/v1/document/{target_id}")
    assert response.status_code == 202

    async with TestAsyncSessionLocal() as verification_session:
        check_db = await verification_session.get(DocumentModel, target_id)
        assert check_db is None, f"Database Leakage: Document {target_id} still exists in PostgreSQL!"

    assert len(mock_kafka.sent_messages) == 1
    assert mock_kafka.sent_messages[0]["topic"] == settings.KAFKA_TOPIC
    assert mock_kafka.sent_messages[0]["key"] == str(target_id).encode("utf-8")


@pytest.mark.asyncio
async def test_invalid_or_missing_document_returns_404(client):
    """Ensures edge-case targets or non-existent primary keys yield an HTTP 404 response."""
    response = await client.delete("/api/v1/document/9999999")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_whitespace_queries_bypass_storage_engines(client):
    """Verifies that empty strings or padded whitespace strings drop out immediately with empty lists."""
    response = await client.get("/api/v1/search?query=      ")
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_redis_cache_hit_and_eviction_lifecycle(client, raw_csv_documents):
    """
    GIVEN a document cached during a search query
    WHEN the same query is executed again
    THEN it must return a Cache Hit without touching the underlying search index.
    WHEN a deletion occurs
    THEN it must evict the stale cache records completely.
    """
    assert len(raw_csv_documents) > 0, "Error: data.csv must contain mock records."

    es_client = create_elastic_client()
    test_index = settings.ELASTIC_INDEX

    # 1. Clear out old tables to start with a clean test slate
    async with TestAsyncSessionLocal() as session:
        from sqlalchemy import text
        await session.execute(text("TRUNCATE TABLE docs CASCADE;"))
        await session.commit()
    await es_client.delete_by_query(index=test_index, body={"query": {"match_all": {}}})

    # 2. Seed a standalone document from data.csv into PostgreSQL and Elasticsearch
    target_data = raw_csv_documents[0]
    target_id = target_data["id"]

    async with TestAsyncSessionLocal() as session:
        db_doc = DocumentModel(
            id=target_id,
            rubrics=target_data["rubrics"],
            text=target_data["text"],
            created_date=target_data["created_date"]
        )
        session.add(db_doc)
        await session.commit()

    await es_client.index(index=test_index, id=str(target_id), body={"text": target_data["text"]})
    await es_client.indices.refresh(index=test_index)

    # 3. First request (Cache Miss): Queries Elasticsearch and caches the result
    first_response = await client.get("/api/v1/search?query=документ")
    assert first_response.status_code == 200
    first_payload = first_response.json()
    assert len(first_payload) > 0

    # 4. Simulate a database change by deleting the index entry behind the app's back
    await es_client.delete(index=test_index, id=str(target_id))
    await es_client.indices.refresh(index=test_index)

    # 5. Second request (Cache Hit): The query should still return the data from Redis
    second_response = await client.get("/api/v1/search?query=документ")
    assert second_response.status_code == 200
    assert second_response.json() == first_payload, "Cache Failure: Data should be loaded from Redis!"

    # 6. Delete the document via the API to trigger a cache eviction
    delete_response = await client.delete(f"/api/v1/document/{target_id}")
    assert delete_response.status_code == 202

    # 7. Third request (Post-Eviction Check): The cache should be cleared, returning an empty list
    third_response = await client.get("/api/v1/search?query=документ")
    assert third_response.status_code == 200
    assert third_response.json() == [], "Eviction Failure: Stale cache keys were not removed from Redis!"

    await es_client.close()


@pytest.mark.asyncio
async def test_api_graceful_degradation_when_redis_is_offline(client, raw_csv_documents):
    """
    GIVEN that the Redis infrastructure cluster goes completely offline
    WHEN a user submits a valid search query to the API endpoint
    THEN the API must degrade gracefully, bypassing the cache layer,
         and successfully return sorted records directly from the database.
    """
    assert len(raw_csv_documents) > 0, "Error: data.csv must contain mock records."

    es_client = create_elastic_client()
    test_index = settings.ELASTIC_INDEX

    # 1. Clear out old tables to start with a clean test slate
    async with TestAsyncSessionLocal() as session:
        from sqlalchemy import text
        await session.execute(text("TRUNCATE TABLE docs CASCADE;"))
        await session.commit()
    await es_client.delete_by_query(index=test_index, body={"query": {"match_all": {}}})

    # 2. Seed a standalone document from data.csv into PostgreSQL and Elasticsearch
    target_data = raw_csv_documents[0]
    target_id = target_data["id"]

    async with TestAsyncSessionLocal() as session:
        db_doc = DocumentModel(
            id=target_id,
            rubrics=target_data["rubrics"],
            text=target_data["text"],
            created_date=target_data["created_date"]
        )
        session.add(db_doc)
        await session.commit()

    await es_client.index(index=test_index, id=str(target_id), body={"text": target_data["text"]})
    await es_client.indices.refresh(index=test_index)

    # 3. Create a broken Redis client double that simulates a total network blackout
    class BrokenRedisMock:
        async def get(self, *args, **kwargs):
            from redis.exceptions import ConnectionError
            raise ConnectionError("Connection refused by target host (Simulated Downtime)")

        async def set(self, *args, **kwargs):
            from redis.exceptions import ConnectionError
            raise ConnectionError("Write command timed out (Simulated Downtime)")

        async def close(self, *args, **kwargs):
            pass

    # 4. Inject the broken Redis double into the app's dependency system on the fly
    async def _get_broken_redis():
        yield BrokenRedisMock()

    from app.database import get_redis_ctx
    from app.main import app
    app.dependency_overrides[get_redis_ctx] = _get_broken_redis

    # 5. Execute the search request over the broken network environment
    response = await client.get("/api/v1/search?query=документ")

    # 6. Verify that the system degrades gracefully instead of throwing an HTTP 500 error
    assert response.status_code == 200, "Resilience Failure: API crashed when Redis went offline!"

    payload = response.json()
    assert len(payload) > 0, "Resilience Failure: API returned empty dataset instead of pulling from DB!"
    assert payload[0]["id"] == target_id, "Resilience Failure: Data payload corrupted during fallback routing!"

    await es_client.close()
