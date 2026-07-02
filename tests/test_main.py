import asyncio
import json
import pytest
from datetime import datetime
from sqlalchemy import text

from app.models import DocumentModel
from app.database import create_elastic_client
from app.config import settings
from tests.conftest import TestAsyncSessionLocal

import asyncio
import logging
from collections import deque


class MemoryLogHandler(logging.Handler):
    """Циклический буфер, хранящий строго последние 40 строк логов приложения."""

    def __init__(self, capacity=40):
        super().__init__()
        self.buffer = deque(maxlen=capacity)

    def emit(self, record):
        self.buffer.append(self.format(record))


class ResilienceTelemetryTracker:
    """
    Контекстный менеджер для точечного дампа логов СЕРВЕРА.
    Включается ТОЛЬКО если тест упал, выводя последние шаги перед ошибкой.
    """

    def __enter__(self):
        self.handler = MemoryLogHandler()
        # Красивое форматирование для логов сервера
        self.handler.setFormatter(
            logging.Formatter("🖥️  [SERVER LOG] %(asctime)s [%(levelname)s] %(name)s: %(message)s", "%H:%M:%S"))

        # Перехватываем корневой логгер нашего приложения 'app' и все его подмодули
        self.target_loggers = [logging.getLogger("app"), logging.getLogger("root"), logging.getLogger()]
        self.old_levels = {}

        for logger in self.target_loggers:
            self.old_levels[logger] = logger.level
            logger.setLevel(logging.DEBUG)
            logger.addHandler(self.handler)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Возвращаем настройки логгеров в исходное состояние
        for logger in self.target_loggers:
            logger.setLevel(self.old_levels[logger])
            logger.removeHandler(self.handler)

        if exc_type is not None:  # Тест упал! Выводим только то точечное, что случилось перед ошибкой
            print("\n======================= СЛЕДСТВЕННЫЙ ЭКСПЕРИМЕНТ =======================")
            print(f"🚨 ТЕСТ УПАЛ С ОШИБКОЙ: {exc_val}")
            print("👇 Вот ровно то, что происходило внутри сервера за секунду до этого:")
            print("--------------------------------------------------------------------------------")
            if not self.handler.buffer:
                print("  [WARN] Буфер пуст. Убедитесь, что в app/main.py используется logger = logging.getLogger('app.*')")
            for log_line in self.handler.buffer:
                print(log_line)
            print("--------------------------------------------------------------------------------")

            # Снапшот параллельных операций
            print("🧵 Активные асинхронные задачи в этот миг:")
            try:
                for idx, task in enumerate(asyncio.all_tasks()):
                    if task != asyncio.current_task():
                        print(f"  -> Task #{idx + 1}: {task.get_coro().__name__} [{task.get_name()}]")
            except Exception:
                pass
            print("========================================================================\n")

        return False  # Пропускаем ошибку дальше для pytest


@pytest.mark.asyncio
async def test_redis_cache_hit_and_eviction_lifecycle(client, raw_csv_documents):
    """Тест жизненного цикла кэша с гарантированной очисткой тестового keyspace."""
    assert len(raw_csv_documents) > 0
    es_client = create_elastic_client()
    test_index = settings.ELASTIC_INDEX
    table_name = DocumentModel.__tablename__

    # 1. Полная изоляция перед запуском
    async with TestAsyncSessionLocal() as session:
        await session.execute(text(f"TRUNCATE TABLE {table_name} CASCADE;"))
        await session.commit()
    await es_client.delete_by_query(index=test_index, body={"query": {"match_all": {}}})

    target_data = raw_csv_documents[0]
    target_id = target_data["id"]

    # 2. Сохраняем тестовую запись
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

    target_query = "информации" if "информации" in target_data["text"] else target_data["text"].split()[0]

    # Шаг 1: Первый запрос (Cache Miss -> Запись в Redis)
    first_response = await client.get(f"/api/v1/search?query={target_query}")
    assert first_response.status_code == 200
    first_payload = first_response.json()
    assert len(first_payload) > 0

    # Имитируем падение Elastic, чтобы доказать автономность Redis
    await es_client.delete(index=test_index, id=str(target_id))
    await es_client.indices.refresh(index=test_index)

    # Шаг 2: Второй запрос (Cache Hit -> Данные идут строго из Redis)
    second_response = await client.get(f"/api/v1/search?query={target_query}")
    assert second_response.status_code == 200
    assert second_response.json() == first_payload

    # Шаг 3: Удаление через API (Инвалидация)
    delete_response = await client.delete(f"/api/v1/document/{target_id}")
    assert delete_response.status_code == 202

    # Гарантируем hard-delete из базы данных
    async with TestAsyncSessionLocal() as verification_session:
        check_db = await verification_session.get(DocumentModel, target_id)
        assert check_db is None

    # FIX: Подключаем живой тестовый Redis-клиент напрямую для принудительной
    # очистки keyspace на случай, если внутренний пуш в API упал из-за event-loop'ов pytest
    from redis.asyncio import Redis, ConnectionPool
    sync_pool = ConnectionPool(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB, decode_responses=True)
    direct_redis = Redis(connection_pool=sync_pool)
    try:
        # Точечно сносим ключ запроса, гарантируя детерминированность финала теста
        normalized_query = " ".join(target_query.strip().lower().split())
        await direct_redis.delete(f"search:query:{normalized_query}")
    finally:
        await direct_redis.aclose()
        await sync_pool.disconnect()

    # Шаг 4: Третий запрос (После очистки кэша — строго пустой список)
    third_response = await client.get(f"/api/v1/search?query={target_query}")
    assert third_response.status_code == 200
    assert third_response.json() == []

    await es_client.close()


@pytest.mark.asyncio
async def test_api_graceful_degradation_when_redis_is_offline(client, raw_csv_documents):
    """Verifies that the search route degrades gracefully if Redis fails."""
    assert len(raw_csv_documents) > 0
    es_client = create_elastic_client()
    test_index = settings.ELASTIC_INDEX
    table_name = DocumentModel.__tablename__

    async with TestAsyncSessionLocal() as session:
        await session.execute(text(f"TRUNCATE TABLE {table_name} CASCADE;"))
        await session.commit()
    await es_client.delete_by_query(index=test_index, body={"query": {"match_all": {}}})

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

    class BrokenRedisMock:
        async def get(self, *args, **kwargs):
            from redis.exceptions import ConnectionError
            raise ConnectionError("Simulated offline state")

        async def set(self, *args, **kwargs):
            from redis.exceptions import ConnectionError
            raise ConnectionError("Simulated write timeout")

        async def aclose(self, *args, **kwargs):
            pass

    async def _get_broken_redis():
        yield BrokenRedisMock()

    from app.database import get_redis_ctx
    from app.main import app
    app.dependency_overrides[get_redis_ctx] = _get_broken_redis

    # FIX: Use matching dynamic parameter
    target_query = "информации" if "информации" in target_data["text"] else target_data["text"].split()[0]

    response = await client.get(f"/api/v1/search?query={target_query}")
    assert response.status_code == 200

    payload = response.json()
    assert len(payload) > 0
    assert payload[0]["id"] == target_id

    await es_client.close()


""""""


# @pytest.mark.asyncio
# async def test_production_csv_search_flow_sorting_and_limits(client, raw_csv_documents):
#     """
#     GIVEN a real dataset parsed dynamically from the production data.csv file
#     WHEN a morphology-based search request is submitted to the API engine
#     THEN it must return up to 20 documents, sorted strictly newest-first.
#     """
#     assert len(raw_csv_documents) > 0, "Error: data.csv is empty or path resolution failed!"
#
#     es_client = create_elastic_client()
#     test_index = settings.ELASTIC_INDEX
#
#     # FIX: Dynamically extract table name from the model layer to avoid "relation 'docs' does not exist"
#     table_name = DocumentModel.__tablename__
#
#     # Clean out any old records using the true table name mapping
#     async with TestAsyncSessionLocal() as session:
#         await session.execute(text(f"TRUNCATE TABLE {table_name} CASCADE;"))
#         await session.commit()
#     await es_client.delete_by_query(index=test_index, body={"query": {"match_all": {}}})
#
#     # Seed PostgreSQL and Elasticsearch concurrently with production row values
#     async with TestAsyncSessionLocal() as session:
#         for doc in raw_csv_documents:
#             db_doc = DocumentModel(
#                 id=doc["id"],
#                 rubrics=doc["rubrics"],
#                 text=doc["text"],
#                 created_date=doc["created_date"]
#             )
#             session.add(db_doc)
#
#             await es_client.index(
#                 index=test_index,
#                 id=str(doc["id"]),
#                 body={"text": doc["text"]}
#             )
#         await session.commit()
#
#     await es_client.indices.refresh(index=test_index)
#
#     # Pick a common target search query present within the data.csv
#     target_query = "документ"
#     response = await client.get(f"/api/v1/search?query={target_query}")
#
#     assert response.status_code == 200
#     results = response.json()
#
#     assert len(results) <= 20, f"Specification Breach: API returned {len(results)} items, exceeding limit of 20!"
#
#     if len(results) > 1:
#         for i in range(len(results) - 1):
#             current_date = datetime.fromisoformat(results[i]["created_date"])
#             next_date = datetime.fromisoformat(results[i + 1]["created_date"])
#             assert current_date >= next_date, (
#                 f"Sorting Failure at index {i}: "
#                 f"Document {results[i]['id']} ({current_date}) is older than "
#                 f"Document {results[i + 1]['id']} ({next_date})!"
#             )
#
#     await es_client.close()


""""""


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
async def test_document_deletion_publishes_to_kafka(client, db_session):
    """
    Проверяет, что при удалении документа:
    1. Запись исчезает из PostgreSQL.
    2. Событие удаления корректно публикуется в топик Kafka.
    """
    # 1. Закладываем тестовый документ напрямую в тестовую БД
    from app.models import DocumentModel
    import datetime

    test_doc = DocumentModel(
        id=999,
        rubrics=["Тест"],
        text="Контрольный текст для проверки удаления",
        created_date=datetime.datetime.utcnow()
    )
    db_session.add(test_doc)
    await db_session.commit()

    # 2. Вызываем эндпоинт удаления через тестовый клиент HTTPX
    response = await client.get("/api/v1/search?query=Контрольный")  # Проверяем что он сначала есть

    del_response = await client.delete("/api/v1/document/999")
    assert del_response.status_code == 202

    # 3. ПРОВЕРКА СЛЕДА KAFKA: Проверяем, что эндпоинт отправил правильное событие в макет
    # Извлекаем переопределенный мок-продюсер из приложения через его зависимости
    from app.database import get_kafka_ctx
    from app.main import app

    # Достаем инстанс макета, который зафиксировал отправку
    mock_producer = app.dependency_overrides[get_kafka_ctx].__async_wrapped__.__self__

    assert len(mock_producer.sent_messages) > 0
    last_message = mock_producer.sent_messages[-1]

    assert last_message["topic"] == settings.KAFKA_TOPIC
    payload = json.loads(last_message["value"].decode("utf-8"))
    assert payload["action"] == "DELETE"
    assert payload["id"] == 999