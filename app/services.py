import json
import datetime
from typing import List
from fastapi import HTTPException, status

from aiokafka import AIOKafkaProducer

from app.models import DocumentResponse
from app.repositories import PostgresRepository, ElasticRepository
from app.config import settings


# =====================================================================
# 4. SERVICES (Business Logic Layer)
# =====================================================================
class DocumentService:
    def __init__(
            self,
            pg_repo: PostgresRepository,
            es_repo: ElasticRepository,
            kafka_producer: AIOKafkaProducer
    ):
        self.pg_repo = pg_repo
        self.es_repo = es_repo
        self.kafka_producer = kafka_producer

    async def search_documents(self, query_text: str) -> List[DocumentResponse]:
        """Search text query: Elastic IDs -> Postgres Docs (with consistency warning tracking)."""
        if not query_text.strip():
            return []

        matched_ids = await self.es_repo.find_ids_by_text(
            query_text,
            limit=settings.ELASTIC_SEARCH_LIMIT
        )
        if not matched_ids:
            return []

        db_rows = await self.pg_repo.get_by_ids_sorted(matched_ids, limit=settings.DB_DISPLAY_LIMIT)

        # Self-Healing & Tracing Fix: Log a warning if Postgres yields fewer documents
        # than what Elastic returned (indicates a background deletion queue lag)
        if len(db_rows) < min(len(matched_ids), settings.DB_DISPLAY_LIMIT):
            returned_ids = {row.id for row in db_rows}
            missing_ids = [idx for idx in matched_ids[:settings.DB_DISPLAY_LIMIT] if idx not in returned_ids]
            print(
                f"WARN: Eventual consistency delay detected. Document IDs {missing_ids} found in index but missing from DB.")

        return [
            DocumentResponse(
                id=row.id,
                rubrics=row.rubrics,
                text=row.text,
                created_date=row.created_date
            ) for row in db_rows
        ]

    async def mark_document_for_deletion(self, doc_id: int) -> None:
        """
        Deletes a document from the database and safely triggers a Kafka event.
        Optimized: Transaction commits first, preventing open DB locks during network I/O.
        """
        # 1. Scope the DB transaction closely
        async with self.pg_repo.session.begin():
            is_deleted = await self.pg_repo.delete_by_id(doc_id)
            if not is_deleted:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Document with ID {doc_id} not found."
                )

        # 2. Fire-and-wait Kafka event OUTSIDE the Postgres transaction context
        try:
            payload = {
                "action": "DELETE",
                "document_id": doc_id,
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            await self.kafka_producer.send_and_wait(
                topic=settings.KAFKA_TOPIC,
                key=str(doc_id).encode("utf-8"),
                value=json.dumps(payload).encode("utf-8")
            )
        except Exception as kafka_err:
            # In a production environment, you would route failed events to a local outbox retry queue
            print(f"Non-blocking error sending deletion event to Kafka for ID {doc_id}: {kafka_err}")


