# app/worker.py
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

# Событие asyncio для безопасного управления жизненным циклом воркера
shutdown_event = asyncio.Event()

logger = logging.getLogger("app.worker")


def handle_exit_signal(sig, frame):
    """Перехватывает SIGTERM/SIGINT от Docker/K8s и взводит событие остановки."""
    print(f"\n[Worker] Received signal ({signal.Signals(sig).name}). Initiating graceful shutdown...")
    # Так как сигнал обрабатывается синхронно, используем thread-safe метод взвода события
    asyncio.get_event_loop().call_soon_threadsafe(shutdown_event.set)


# app/worker.py

# ... (все ваши импорты и handle_exit_signal остаются без изменений) ...

async def start_kafka_consumer_worker():
    """Фоновый процесс с механизмом безопасного ожидания готовности брокера Kafka."""
    consumer = AIOKafkaConsumer(
        settings.KAFKA_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )

    es_client: AsyncElasticsearch = create_elastic_client()
    redis_client = Redis(connection_pool=redis_pool)

    # МЕХАНИЗМ RETRY ДЛЯ ПОДКЛЮЧЕНИЯ К KAFKA (Защита от Race Condition)
    max_retries = 10
    retry_interval = 3  # секунды

    for attempt in range(1, max_retries + 1):
        if shutdown_event.is_set():
            break
        try:
            logger.info(f"📟 [WORKER] Connection attempt {attempt}/{max_retries} to Kafka cluster...")
            await consumer.start()
            logger.info("📟 [WORKER] Kafka synergy consumer loop initialized and listening successfully!")
            break
        except Exception as conn_err:
            if attempt == max_retries:
                logger.error("❌ [WORKER] Critical failure: Kafka broker is unavailable after maximum retries.")
                # Чистим за собой коннекты перед жестким падением
                await es_client.close()
                await redis_client.close()
                raise conn_err

            logger.warning(f"⚠️ [WORKER] Kafka port 9092 is not ready yet: {conn_err}. Retrying in {retry_interval}s...")
            await asyncio.sleep(retry_interval)

    # Основной цикл обработки сообщений
    try:
        while not shutdown_event.is_set():
            try:
                msg_batch = await consumer.getmany(timeout_ms=1000)

                for tp, messages in msg_batch.items():
                    for message in messages:
                        payload = json.loads(message.value.decode("utf-8"))
                        action = payload.get("action")
                        doc_id = payload.get("id")

                        logger.info(f"⚡ [WORKER] Processing mutation event: {action} on resource ID {doc_id}")

                        if action == "DELETE":
                            try:
                                await es_client.delete(index=settings.ELASTIC_INDEX, id=str(doc_id))
                                logger.info(f"[WORKER] Successfully removed Document ID {doc_id} from Elasticsearch.")
                            except Exception as es_err:
                                logger.warning(f"[WORKER] Elastic entry already missing or deleted: {es_err}")

                            await invalidate_search_cache(redis_client)

            except Exception as msg_err:
                logger.error(f"[WORKER] Error during message processing batch lookup: {msg_err}")

    finally:
        logger.info("[Worker] Releasing infrastructure connections...")
        # Останавливаем consumer только если он был успешно запущен
        try:
            await consumer.stop()
        except Exception:
            pass
        await es_client.close()
        await redis_client.close()
        logger.info("[Worker] Graceful shutdown completed successfully.")


if __name__ == "__main__":
    import signal

    signal.signal(signal.SIGINT, handle_exit_signal)
    signal.signal(signal.SIGTERM, handle_exit_signal)

    try:
        asyncio.run(start_kafka_consumer_worker())
    except Exception as entry_err:
        print(f"Worker bootstrap fatal failure: {entry_err}", file=sys.stderr)
        sys.exit(1)

