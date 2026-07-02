import json
import logging

import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, status, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from elasticsearch import AsyncElasticsearch
from redis.asyncio import Redis

# Import your new dependencies and services
from app.database import get_db_session, get_redis_ctx, create_elastic_client, verify_and_setup_elastic_index, \
    create_kafka_producer, redis_pool, get_elastic_ctx, get_kafka_ctx
from app.config import settings
from app.redis_service import get_cached_search_results, set_search_results_cache, invalidate_search_cache

from app.repositories import PostgresRepository, ElasticRepository
from app.services import DocumentService
from app.models import DocumentResponse, MessageResponse, DocumentModel

logger = logging.getLogger("app.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages system startup and shutdown loops gracefully."""
    logger.info("Booting up API service layer mesh...")
    yield
    logger.info("Shutting down API service layer mesh...")
    # Cleanly disconnect connection pools to prevent network descriptor leakage
    await redis_pool.disconnect()
    logger.info("Redis connection pools released successfully.")


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
    """
    Executes a resilient read-through lookup sequence.
    Utilizes clean, modern Elasticsearch 9.x search payload parameters
    to permanently clear all keyword overlap deprecation warnings.
    """
    cleaned_query = query.strip()
    if not cleaned_query:
        return []

    # Step A: Protected read pass from the caching layer
    cached_payload = await get_cached_search_results(redis, cleaned_query)
    if cached_payload is not None:
        return cached_payload

    logger.info(f"🔍 [CACHE MISS] Routing search token '{cleaned_query}' to backend engines.")

    # Step B: Standardize modern Elasticsearch query parameters
    # FIX: Merged the size attribute directly inside the body payload array to remove deprecations
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

    matched_ids = []
    for hit in hits:
        hit_id = hit.get("_id")
        source_id = hit.get("_source", {}).get("id")
        logger.info(f"🔍 [MAIN API] Трейс Хита -> Системный _id: '{hit_id}', ID в _source: '{source_id}'")
        if hit_id is not None:
            matched_ids.append(int(hit_id))

    # Fetch complete relational records matching targeted criteria
    from sqlalchemy import select
    stmt = select(DocumentModel).where(DocumentModel.id.in_(matched_ids))
    db_result = await db.execute(stmt)
    documents = db_result.scalars().all()

    # Enforce priority creation date sorting sequence and cap limits at 20 rows
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

    # Step C: Hydrate cache safely
    await set_search_results_cache(redis, cleaned_query, response_payload, ttl=settings.CACHE_TTL_SECONDS)

    return response_payload


@app.delete("/api/v1/document/{document_id}", status_code=status.HTTP_202_ACCEPTED)
async def delete_document_endpoint(
    document_id: int,
    db: AsyncSession = Depends(get_db_session),
    kafka_producer=Depends(get_kafka_ctx)
):
    """
    Deletes the resource from the relational database and offloads sync tasks to Kafka.
    Redis eviction is handled asynchronously by the Kafka background consumer loop.
    """
    doc = await db.get(DocumentModel, document_id)
    if not doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Document with ID {document_id} not found."
        )

    await db.delete(doc)
    await db.commit()

    await kafka_producer.send_and_wait(
        settings.KAFKA_TOPIC,
        key=str(document_id).encode("utf-8"),
        value=json.dumps({"action": "DELETE", "id": document_id}).encode("utf-8")
    )

    return {"detail": f"Document deletion task submitted for ID {document_id}."}


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8000,
        reload=True
    )
