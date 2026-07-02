import random
from locust import HttpUser, task, between


class SearchEngineLoadUser(HttpUser):
    # Каждый виртуальный пользователь ждет от 0.1 до 0.5 сек между запросами
    wait_time = between(0.1, 0.5)

    @task(8)
    def search_documents(self):
        """Имитирует постоянный конкурентный поиск по разным ключевым словам."""
        queries = ["документ", "авиация", "политика", "критический", "система", "тест"]
        target_query = random.choice(queries)

        # Специально миксуем запросы, чтобы часть шла из кэша Redis, а часть вызывала Cache Miss
        if random.random() > 0.5:
            # Уникальная соль пробивает Redis-кэш и бьет напрямую в Elasticsearch
            target_query = f"{target_query}_{random.randint(1, 1000)}"

        with self.client.get(
                f"/api/v1/search?query={target_query}",
                name="/api/v1/search",
                catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            elif response.status_code == 503:
                # Если упал Elastic, помечаем в отчете как терпимый сбой платформы
                response.failure("Search Platform Degraded (503)")

    @task(2)
    def delete_document_flow(self):
        """Имитирует редкие, но тяжелые запросы на удаление."""
        # Генерируем случайный ID для удаления
        random_doc_id = random.randint(1, 10000)

        with self.client.delete(
                f"/api/v1/document/{random_doc_id}",
                name="/api/v1/document/{id}",
                catch_response=True
        ) as response:
            # 202 - успешно улетело в Kafka, 404 - документа уже нет (тоже успех для нагрузки)
            if response.status_code in (200, 202, 404):
                response.success()
            else:
                response.failure(f"Unexpected deletion error status: {response.status_code}")
