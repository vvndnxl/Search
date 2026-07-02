import logging
import sys
import signal
import asyncio
import json
from aiokafka import AIOKafkaConsumer
from elasticsearch import AsyncElasticsearch
from redis.asyncio import Redis

from app.config import settings
from app.database import create_elastic_client, redis_pool
from app.redis_service import invalidate_search_cache
from app.repositories import ElasticRepository

# Global shutdown state tracker
is_running = True

logger = logging.getLogger("app.worker")


def handle_exit_signal(sig, frame):
    """Intercepts system termination requests (SIGTERM/SIGINT) to trigger a clean exit."""
    global is_running
    print(f"\n[Worker] Received shutdown signal ({signal.Signals(sig).name}). Stopping event loop gracefully...")
    is_running = False


async def start_kafka_consumer_worker():
    """Background loop listening for change events to sync Elasticsearch and Redis caches."""
    consumer = AIOKafkaConsumer(
        settings.KAFKA_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id="search-sync-group",
        auto_offset_reset="earliest"
    )

    es_client: AsyncElasticsearch = create_elastic_client()
    redis_client = Redis(connection_pool=redis_pool)

    await consumer.start()
    logger.info("📟 [WORKER] Kafka synergy consumer loop initialized and listening...")

    try:
        async for message in consumer:
            try:
                payload = json.loads(message.value.decode("utf-8"))
                action = payload.get("action")
                doc_id = payload.get("id")

                logger.info(f"⚡ [WORKER] Processing mutation event: {action} on resource ID {doc_id}")

                if action == "DELETE":
                    # 1. Update primary search indices
                    try:
                        await es_client.delete(index=settings.ELASTIC_INDEX, id=str(doc_id))
                        logger.info(f"[WORKER] Successfully removed Document ID {doc_id} from Elasticsearch.")
                    except Exception as es_err:
                        logger.warning(f"[WORKER] Elastic entry already missing or deleted: {es_err}")

                    # 2. Synergy point: Protected cache eviction block
                    # If Redis fails, the exception is caught within the service,
                    # ensuring the worker doesn't crash and can process subsequent Kafka events.
                    await invalidate_search_cache(redis_client)


            except Exception as msg_err:
                logger.error(f"[WORKER] Failed to process incoming event payload: {msg_err}")
    finally:
        await consumer.stop()
        await es_client.close()
        await redis_client.close()


if __name__ == "__main__":
    try:
        asyncio.run(start_kafka_consumer_worker())
    except Exception as entry_err:
        print(f"Worker bootstrap fatal failure: {entry_err}", file=sys.stderr)
        sys.exit(1)
