import json
import logging
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, status, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from elasticsearch import AsyncElasticsearch
from redis.asyncio import Redis

from app.database import get_db_session, get_redis_ctx, create_elastic_client, redis_pool, get_elastic_ctx, get_kafka_ctx, init_infrastructure, close_infrastructure
from app.config import settings
from app.redis_service import get_cached_search_results, set_search_results_cache
from app.models import DocumentResponse, MessageResponse, DocumentModel

logger = logging.getLogger("app.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages system startup and shutdown loops gracefully."""
    logger.info("Booting up API service layer mesh...")

    # Интегрируем жесткий Fail-Fast контроль синглтонов инфраструктуры при старте
    try:
        await init_infrastructure()
    except Exception as infra_err:
        logger.critical(f"💥 [FATAL STARTUP BLOCKED] Infrastructure mesh failed to align: {infra_err}")
        raise infra_err  # Роняем приложение, не запуская uvicorn

    yield

    logger.info("Shutting down API service layer mesh...")

    # Безопасно закрываем глобальные синглтоны Elastic и Kafka
    await close_infrastructure()

    # Сохраняем вашу оригинальную логику очистки пулов Redis
    try:
        await redis_pool.disconnect()
        logger.info("Redis connection pools released successfully.")
    except Exception as e:
        logger.warning(f"Error disconnecting Redis pool during shutdown: {e}")

app = FastAPI(
    title=settings.PROJECT_NAME,
    version="1.0.0",
    lifespan=lifespan,
    description="Asynchronous document search engine powered by FastAPI, Kafka, and Elasticsearch"
)


@app.get("/api/v1/search", response_model=list[DocumentResponse])
async def search_documents_endpoint(
        query: str = Query(..., min_length=1),
        db: AsyncSession = Depends(get_db_session),
        es: AsyncElasticsearch = Depends(get_elastic_ctx),
        redis: Redis = Depends(get_redis_ctx)
):
    cleaned_query = query.strip()
    if not cleaned_query:
        return []

    cached_payload = await get_cached_search_results(redis, cleaned_query)
    if cached_payload is not None:
        return cached_payload

    logger.info(f"🔍 [CACHE MISS] Routing search token '{cleaned_query}' to backend engines.")

    search_body = {
        "size": 100,
        "query": {
            "match": {
                "text": {"query": cleaned_query, "analyzer": "russian"}
            }
        }
    }

    try:
        es_response = await es.search(index=settings.ELASTIC_INDEX, body=search_body)
        hits = es_response["hits"]["hits"]
    except Exception as es_err:
        logger.error(f"❌ [CRITICAL ENGINE FAILURE] Elasticsearch cluster unreachable: {es_err}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Search index platform engine is temporarily unavailable."
        )

    if not hits:
        await set_search_results_cache(redis, cleaned_query, [], ttl=settings.CACHE_TTL_SECONDS)
        return []

    matched_ids = [int(hit["_id"]) for hit in hits]

    from sqlalchemy import select
    stmt = select(DocumentModel).where(DocumentModel.id.in_(matched_ids))
    db_result = await db.execute(stmt)
    documents = db_result.scalars().all()

    sorted_docs = sorted(documents, key=lambda d: d.created_date, reverse=True)[:20]

    response_payload = [
        {
            "id": doc.id,
            "rubrics": doc.rubrics,
            "text": doc.text,
            "created_date": doc.created_date.isoformat()
        }
        for doc in sorted_docs
    ]

    await set_search_results_cache(redis, cleaned_query, response_payload, ttl=settings.CACHE_TTL_SECONDS)
    return response_payload


@app.delete("/api/v1/document/{document_id}", status_code=status.HTTP_202_ACCEPTED)
async def delete_document_endpoint(
    document_id: int,
    db: AsyncSession = Depends(get_db_session),
    kafka_producer=Depends(get_kafka_ctx)
):
    doc = await db.get(DocumentModel, document_id)
    if not doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Document with ID {document_id} not found."
        )

    # Шаг 1: Удаляем из основного источника правды (Postgres ACID транзакция)
    await db.delete(doc)
    await db.commit()

    # Шаг 2: Попытка асинхронной публикации в Kafka (с обработкой падения брокера)
    try:
        await kafka_producer.send_and_wait(
            settings.KAFKA_TOPIC,
            key=str(document_id).encode("utf-8"),
            value=json.dumps({"action": "DELETE", "id": document_id}).encode("utf-8")
        )
        return {"detail": f"Document deletion task submitted for ID {document_id}."}
    except Exception as kafka_err:
        # Graceful Degradation: БД очищена, но синхронизация ES отложена
        logger.critical(
            f"⚠️ [KAFKA OFFLINE] Failed to stream delete event for ID {document_id}: {kafka_err}. "
            f"Database mutation succeeded, but sync queue requires manual replay or DLQ processing."
        )
        return {
            "detail": f"Document ID {document_id} removed from DB, but sync queue was offline. Changes will reflect asynchronously later."
        }


@app.get("/api/v1/health", status_code=status.HTTP_200_OK)
async def health_check_endpoint(
    db: AsyncSession = Depends(get_db_session),
    es: AsyncElasticsearch = Depends(get_elastic_ctx),
    redis: Redis = Depends(get_redis_ctx)
):
    """Комплексный healthcheck с разделением на критические и терпимые сбои."""
    health_status = {
        "status": "healthy",
        "services": {"postgres": "unhealthy", "elasticsearch": "unhealthy", "redis": "unhealthy"}
    }
    critical_error = False

    # 1. Проверка критической Postgres
    try:
        from sqlalchemy import text
        await db.execute(text("SELECT 1"))
        health_status["services"]["postgres"] = "healthy"
    except Exception as e:
        health_status["services"]["postgres"] = f"unhealthy: {str(e)}"
        critical_error = True

    # 2. Проверка критического Elasticsearch
    try:
        es_health = await es.cluster.health()
        health_status["services"]["elasticsearch"] = f"healthy (status: {es_health.get('status')})"
    except Exception as e:
        health_status["services"]["elasticsearch"] = f"unhealthy: {str(e)}"
        critical_error = True

    # 3. Проверка МЯГКОЙ зависимости Redis (Не роняет весь API)
    try:
        await redis.ping()
        health_status["services"]["redis"] = "healthy"
    except Exception as e:
        health_status["services"]["redis"] = f"unhealthy (degraded state): {str(e)}"
        # Если лежит только Redis, переводим статус в degraded, но НЕ взводим critical_error
        if not critical_error:
            health_status["status"] = "degraded"

    if critical_error:
        health_status["status"] = "unhealthy"
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=health_status)

    return health_status


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
