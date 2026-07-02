import json
import logging
from typing import Optional, Any
from redis.asyncio import Redis
from redis.exceptions import RedisError

logger = logging.getLogger("app.redis_service")


async def get_cached_search_results(redis_client: Redis, query: str) -> Optional[list[dict[str, Any]]]:
    """
    Безопасное чтение кэша. Если Redis недоступен, функция перехватывает ошибку,
    возвращает None и позволяет API продолжить запрос в базы данных.
    """
    normalized_query = " ".join(query.strip().lower().split())
    cache_key = f"search:query:{normalized_query}"

    try:
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            logger.info(f"💾 [CACHE HIT] Результаты найдены в кэше по ключу: {cache_key}")
            return json.loads(cached_data)
    except (RedisError, OSError, ConnectionError) as redis_err:
        # Graceful Degradation: логируем сбой сети, но не роняем поисковый эндпоинт
        logger.warning(
            f"⚠️ [REDIS OFFLINE] Ошибка чтения кэша для ключа '{cache_key}': {redis_err}. "
            f"Перенаправление запроса напрямую в хранилища."
        )
    except Exception as unexpected_err:
        logger.error(f"❌ [CACHE ERROR] Непредвиденная ошибка при чтении JSON кэша: {unexpected_err}")

    return None


async def set_search_results_cache(redis_client: Redis, query: str, documents: list[dict[str, Any]], ttl: int = 300) -> None:
    """
    Безопасная запись кэша. Если Redis переполнен (OOM) или отключен,
    запись пропускается, позволяя API штатно выдать ответ пользователю.
    """
    normalized_query = " ".join(query.strip().lower().split())
    cache_key = f"search:query:{normalized_query}"

    try:
        serialized_payload = json.dumps(documents, default=str)
        await redis_client.set(cache_key, serialized_payload, ex=ttl)
        logger.info(f"📥 [CACHE STORED] Результаты успешно кэшированы: {cache_key} (TTL: {ttl}s)")
    except (RedisError, OSError, ConnectionError) as redis_err:
        logger.warning(
            f"⚠️ [REDIS OFFLINE] Не удалось сохранить кэш для '{cache_key}': {redis_err}. "
            f"Система продолжает работу без записи кэша."
        )
    except Exception as unexpected_err:
        logger.error(f"❌ [CACHE ERROR] Непредвиденная ошибка при сериализации кэша: {unexpected_err}")


async def invalidate_search_cache(redis_client: Redis) -> None:
    """
    Безопасная инвалидация кaim-кэша через SCAN. Если очистка падает,
    воркер продолжает работу, чтобы гарантированно обновить Elasticsearch и БД.
    """
    cache_pattern = "search:query:*"
    cursor = 0
    deleted_count = 0

    try:
        logger.info("♻️ [CACHE INVALIDATION] Сканирование и очистка устаревших ключей кэша...")
        while True:
            cursor, keys = await redis_client.scan(cursor=cursor, match=cache_pattern, count=100)
            if keys:
                await redis_client.delete(*keys)
                deleted_count += len(keys)
            if cursor == 0:
                break
        logger.info(f"✨ [CACHE INVALIDATION] Сброс кэша завершен. Удалено ключей: {deleted_count}.")
    except (RedisError, OSError, ConnectionError) as redis_err:
        logger.error(
            f"⚠️ [REDIS OFFLINE] Критический сбой при сбросе кэша поиска: {redis_err}. "
            f"Внимание: возможна временная рассинхронизация кэша до истечения TTL!"
        )
