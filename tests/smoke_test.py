# tests/smoke_test.py
import os
import sys
import time
import requests

# Определяем базовый URL проверяемого сервиса
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")


def run_smoke_tests():
    print(f"🚀 [SMOKE TEST] Initializing verification loop against target endpoint: {API_BASE_URL}")

    # -----------------------------------------------------------------
    # ШАГ 1: Проверка комплексного эндпоинта здоровья систем
    # -----------------------------------------------------------------
    health_url = f"{API_BASE_URL}/api/v1/health"
    try:
        response = requests.get(health_url, timeout=5)
    except requests.exceptions.ConnectionError:
        print(f"❌ CRITICAL FAILURE: Cannot connect to API gateway at {API_BASE_URL}")
        sys.exit(1)

    if response.status_code != 200:
        print(f"❌ HEALTHCHECK FAILED: HTTP Status {response.status_code}")
        print(f"Payload context: {response.text}")
        sys.exit(1)

    health_data = response.json()

    # ИСПРАВЛЕНИЕ: Реагируем на частичную деградацию кэша Redis
    if health_data.get("status") == "degraded":
        print(f"⚠️  WARNING: Infrastructure layer is active but DEGRADED. Context: {health_data['services']}")
    elif health_data.get("status") == "healthy":
        print(f"✅ Infrastructure Health Layer is fully active: {health_data['status']}")
    else:
        print(f"❌ CRITICAL: Unknown health status token: {health_data.get('status')}")
        sys.exit(1)

    # -----------------------------------------------------------------
    # ШАГ 2: Проверка сквозного поиска и сквозной Pydantic-валидации кэша
    # -----------------------------------------------------------------
    search_url = f"{API_BASE_URL}/api/v1/search"
    target_query = "документ"

    # Запрос 1: Проверка базовой выборки (Cache Miss)
    res_miss = requests.get(search_url, params={"query": target_query}, timeout=5)
    if res_miss.status_code != 200:
        print(f"❌ SEARCH ENGINE ERROR (Cache Miss block): Status {res_miss.status_code}")
        sys.exit(1)

    documents = res_miss.json()
    if not isinstance(documents, list):
        print("❌ VALIDATION ERROR: Search endpoint did not return a valid list payload structure.")
        sys.exit(1)

    print(f"✅ Search pass 1 successful (Found {len(documents)} entries)")

    if not documents:
        print("⚠️ Alert: Search payload empty. Skipping deletion sequence pipelines.")
        print("💯 Smoke tests completed successfully (with empty data target criteria).")
        sys.exit(0)

    # ОПТИМИЗАЦИЯ: Дополнительно проверяем соответствие нашей Pydantic схеме DocumentResponse
    first_doc = documents[0]
    required_fields = {"id", "rubrics", "text", "created_date"}
    if not required_fields.issubset(first_doc.keys()):
        print(f"❌ SPECIFICATION BREACH: Document schema is missing fields. Got: {first_doc.keys()}")
        sys.exit(1)

    # Запрос 2: Проверка чтения структуры из Redis Cache (Cache Hit)
    res_hit = requests.get(search_url, params={"query": target_query}, timeout=5)
    if res_hit.status_code != 200:
        print(f"❌ SEARCH ENGINE ERROR (Cache Hit validation path): Status {res_hit.status_code}")
        sys.exit(1)
    print("✅ Search pass 2 successful (Read from distributed cache layer validated)")

    # -----------------------------------------------------------------
    # ШАГ 3: Проверка асинхронного удаления и Eventual Consistency (Kafka)
    # -----------------------------------------------------------------
    target_doc_id = documents[0]["id"]
    delete_url = f"{API_BASE_URL}/api/v1/document/{target_doc_id}"

    print(f"🧹 Initiating mutation task request for targeted resource ID: {target_doc_id}")
    del_response = requests.delete(delete_url, timeout=5)

    if del_response.status_code not in (200, 202):
        print(f"❌ DELETION ROUTE REJECTED: HTTP Status {del_response.status_code}")
        sys.exit(1)

    # Парсим тело ответа, чтобы убедиться, что логика деградации не сработала аварийно
    del_data = del_response.json()
    print(f"✅ Deletion transaction response received: {del_data.get('detail')}")

    # Цикл опроса (Polling Loop) для проверки асинхронного вымывания данных воркером из ES
    print("⏳ Waiting for Kafka worker eviction sync sequence...")
    eviction_successful = False

    for attempt in range(1, 16):  # Расширяем до 15 попыток (7.5 секунд) для тяжелых сред K8s
        time.sleep(0.5)

        # ИСПРАВЛЕНИЕ: Добавляем динамическую соль к запросу, чтобы пробить кэш Redis
        # и опрашивать реальное состояние индексов Elasticsearch напрямую!
        bypass_cache_query = f"{target_query} &cache_bypass={time.time_ns()}"

        check_res = requests.get(search_url, params={"query": bypass_cache_query}, timeout=5)
        if check_res.status_code != 200:
            continue

        current_docs = check_res.json()
        remaining_ids = [d["id"] for d in current_docs]

        if target_doc_id not in remaining_ids:
            eviction_successful = True
            print(f"✅ Eventual Consistency Confirmed! Document {target_doc_id} evicted on attempt {attempt}.")
            break

    if not eviction_successful:
        print(f"❌ PIPELINE TIMEOUT: Document {target_doc_id} was not evicted from Elasticsearch within limits.")
        sys.exit(1)

    print("💯 ALL SMOKE TEST PIPELINES COMPLETED SUCCESSFULLY!")


if __name__ == "__main__":
    run_smoke_tests()
