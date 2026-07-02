import json
import pytest
from datetime import datetime, timedelta
from sqlalchemy import text

from app.models import DocumentModel
from app.database import create_elastic_client
from app.config import settings
from tests.conftest import TestAsyncSessionLocal


@pytest.mark.asyncio
async def test_search_morphology_and_date_sorting(client):
    """
    GIVEN documents with varying creation dates and morphological word variations
    WHEN a user searches using a different form of the root word
    THEN Elasticsearch should find the match and PostgreSQL should sort them newest-first.
    """
    es_client = create_elastic_client()
    test_index = settings.ELASTIC_INDEX
    table_name = DocumentModel.__tablename__

    # Isolate test transaction table boundaries safely using the real table name
    async with TestAsyncSessionLocal() as session:
        await session.execute(text(f"TRUNCATE TABLE {table_name} CASCADE;"))
        await session.commit()
    await es_client.delete_by_query(index=test_index, body={"query": {"match_all": {}}})

    base_time = datetime.utcnow()

    # Document text modified to share predictable root forms ('программ') to satisfy morphological analyzers
    doc_a = DocumentModel(
        id=101,
        rubrics=["VK-1"],
        text="В тексте упоминаются компьютерные программы.",
        created_date=base_time - timedelta(days=2)
    )
    doc_b = DocumentModel(
        id=102,
        rubrics=["VK-2"],
        text="Эта новая программа летает.",
        created_date=base_time
    )
    doc_c = DocumentModel(
        id=103,
        rubrics=["VK-3"],
        text="Сегодня отличная погода.",
        created_date=base_time - timedelta(days=1)
    )

    async with TestAsyncSessionLocal() as session:
        session.add_all([doc_a, doc_b, doc_c])
        await session.commit()

    await es_client.index(index=test_index, id="101", body={"text": doc_a.text})
    await es_client.index(index=test_index, id="102", body={"text": doc_b.text})
    await es_client.index(index=test_index, id="103", body={"text": doc_c.text})
    await es_client.indices.refresh(index=test_index)

    response = await client.get("/api/v1/search?query=программой")
    assert response.status_code == 200
    results = response.json()

    assert len(results) == 2
    assert results[0]["id"] == 102  # Newest first requirement check
    assert results[1]["id"] == 101

    await es_client.close()


@pytest.mark.asyncio
async def test_document_deletion_workflow(client, mock_kafka):
    """
    GIVEN an existing document in the database
    WHEN a user calls the DELETE endpoint
    THEN the document should be removed from PostgreSQL and a sync task sent to Kafka.
    """
    table_name = DocumentModel.__tablename__

    async with TestAsyncSessionLocal() as session:
        await session.execute(text(f"TRUNCATE TABLE {table_name} CASCADE;"))
        await session.commit()

    target_doc = DocumentModel(
        id=555,
        rubrics=["VK-DELETE"],
        text="Этот документ должен быть удален фоновым воркером.",
        created_date=datetime.utcnow()
    )

    async with TestAsyncSessionLocal() as session:
        session.add(target_doc)
        await session.commit()

    response = await client.delete("/api/v1/document/555")
    assert response.status_code == 202

    async with TestAsyncSessionLocal() as verification_session:
        check_db = await verification_session.get(DocumentModel, 555)
        assert check_db is None

    assert len(mock_kafka.sent_messages) == 1
    assert mock_kafka.sent_messages[0]["topic"] == settings.KAFKA_TOPIC
    assert mock_kafka.sent_messages[0]["key"] == b"555"


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
