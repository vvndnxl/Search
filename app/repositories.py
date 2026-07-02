from typing import List

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from elasticsearch import AsyncElasticsearch, NotFoundError

from app.config import settings
from app.models import DocumentModel


# 3. REPOSITORIES (Data Access Layer)
# =====================================================================
class PostgresRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_ids_sorted(self, ids: List[int], limit: int) -> List[DocumentModel]:
        """
        Retrieves complete records matching the input ID array, ordered by date.
        Fixed: Pure ORM query returning actual Model instances, bypassing dynamic row tuple parsing.
        """
        query = (
            select(DocumentModel)
            .where(DocumentModel.id.in_(ids))
            .order_by(DocumentModel.created_date.desc())
            .limit(limit)
        )
        result = await self.session.execute(query)
        # scalars() extracts the DocumentModel object cleanly out of the tracking wrapper
        return list(result.scalars().all())

    async def delete_by_id(self, doc_id: int) -> bool:
        """
        Removes target item.
        Fixed: Standard ORM delete syntax aligned with asynchronous transactional flushes.
        """
        query = (
            delete(DocumentModel)
            .where(DocumentModel.id == doc_id)
            .returning(DocumentModel.id)
        )
        result = await self.session.execute(query)
        deleted_id = result.fetchone()
        return deleted_id is not None


class ElasticRepository:
    def __init__(self, es_client: AsyncElasticsearch):
        self.es_client = es_client

    async def find_ids_by_text(self, query_text: str, limit: int) -> List[int]:
        """
        Полнотекстовый поиск в Elasticsearch.
        Оптимизация: _source=False отключает передачу текста документов по сети.
        """
        try:
            # 9.x requires explicit query-argument mapping blocks
            response = await self.es_client.search(
                index=settings.ELASTIC_INDEX,
                query={"match": {"text": query_text}},
                _source=False,  # Excludes text source payloads to minimize network IO
                size=limit
            )

            # Extract and convert matching hit IDs back to integers
            hits = response["hits"]["hits"]
            return [int(hit["_id"]) for hit in hits]

        except Exception as es_err:
            print(f"Elasticsearch query runtime exception: {es_err}")
            return []

    async def remove_from_index(self, doc_id: int) -> None:
        """Clears elements matching targeted identifier blocks from the index."""
        try:
            await self.es_client.delete(
                index=settings.ELASTIC_INDEX,
                id=str(doc_id)
            )
        except NotFoundError:
            pass
