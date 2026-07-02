import json
import pytest
from datetime import datetime, timedelta

from app.models import DocumentModel
from app.database import create_elastic_client
from app.config import settings
from tests.conftest_base import TestAsyncSessionLocal


@pytest.mark.asyncio
async def test_search_morphology_and_date_sorting(client):
    """Тест поиска с морфологией и сортировкой по дате (с подробным логированием)."""
    es_client = create_elastic_client()
    test_index = settings.ELASTIC_INDEX

    print(f"\n[DEBUG] Очистка и подготовка тестового индекса: {test_index}")
    await es_client.delete_by_query(index=test_index, body={"query": {"match_all": {}}})

    base_time = datetime.utcnow()
    # Document text modified to share predictable root forms ('программ')
    doc_a = DocumentModel(id=101, rubrics=["VK-1"], text="В тексте упоминаются компьютерные программы.",
                          created_date=base_time - timedelta(days=2))
    doc_b = DocumentModel(id=102, rubrics=["VK-2"], text="Эта новая программа летает.", created_date=base_time)
    doc_c = DocumentModel(id=103, rubrics=["VK-3"], text="Сегодня отличная погода.",
                          created_date=base_time - timedelta(days=1))

    print("[DEBUG] Сохранение документов в PostgreSQL...")
    async with TestAsyncSessionLocal() as session:
        session.add_all([doc_a, doc_b, doc_c])
        await session.commit()

    print("[DEBUG] Индексация документов в Elasticsearch...")
    await es_client.index(index=test_index, id="101", body={"text": doc_a.text})
    await es_client.index(index=test_index, id="102", body={"text": doc_b.text})
    await es_client.index(index=test_index, id="103", body={"text": doc_c.text})

    print("[DEBUG] Принудительное обновление индекса (refresh)...")
    await es_client.indices.refresh(index=test_index)

    # ---- БЛОК ДИАГНОСТИКИ СТЕММИНГА ----
    print("\n================== ДИАГНОСТИКА РАБОТЫ АНАЛИЗАТОРА ==================")
    try:
        for text_sample in [doc_a.text, doc_b.text, "программой"]:
            tokens_res = await es_client.indices.analyze(
                index=test_index,
                body={"analyzer": "russian", "text": text_sample}
            )
            tokens = [t["token"] for t in tokens_res["tokens"]]
            print(f"Текст: '{text_sample}' -> Токены: {tokens}")
    except Exception as e:
        print(f"[WARN] Не удалось выполнить analyze: {e}. Возможно, индекс использует дефолтный анализатор.")

    # Проверяем, что вообще физически лежит в Elastic перед запросом
    all_docs = await es_client.search(index=test_index, body={"query": {"match_all": {}}})
    print(f"Всего документов в индексе Elastic: {all_docs['hits']['total']['value']}")
    for hit in all_docs['hits']['hits']:
        print(f" -> ID: {hit['_id']}, Source: {hit['_source']}")
    print("====================================================================\n")

    print("[DEBUG] Выполнение HTTP-запроса к API...")
    response = await client.get("/api/v1/search?query=программой")

    assert response.status_code == 200
    results = response.json()

    print(f"[DEBUG] Ответ от API (найдено {len(results)}): {results}")

    assert len(results) == 2
    assert results[0]["id"] == 102
    assert results[1]["id"] == 101

    await es_client.close()


@pytest.mark.asyncio
async def test_document_deletion_workflow(client, mock_kafka):
    """Verifies rows are dropped from Postgres and sync signals are emitted to Kafka."""
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

    # Open a fresh database session block to verify that the record was deleted cleanly
    async with TestAsyncSessionLocal() as verification_session:
        check_db = await verification_session.get(DocumentModel, 555)
        assert check_db is None

    assert len(mock_kafka.sent_messages) == 1
    assert mock_kafka.sent_messages[0]["topic"] == settings.KAFKA_TOPIC
    assert mock_kafka.sent_messages[0]["key"] == b"555"


@pytest.mark.asyncio
async def test_delete_non_existent_document_returns_404(client):
    """Ensures requests for missing assets result in an HTTP 404 error response code."""
    response = await client.delete("/api/v1/document/999999")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_search_with_empty_query_returns_empty_list(client):
    """Ensures empty user space query bounds bypass hitting storage engines."""
    response = await client.get("/api/v1/search?query=   ")
    assert response.status_code == 200
    assert response.json() == []
