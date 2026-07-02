#!/bin/bash
# scripts/validate_cluster.sh

echo "🕵️ Starting external cluster validation inspection..."

REQUIRED_CONTAINERS=("search_postgres" "search_elastic" "search_redis" "search_kafka" "search_api" "search_worker")
MAX_ATTEMPTS=12
SLEEP_INTERVAL=5

for attempt in $(seq 1 $MAX_ATTEMPTS); do
    all_healthy=true
    echo "Checking cluster state (Attempt $attempt/$MAX_ATTEMPTS)..."

    for container in "${REQUIRED_CONTAINERS[@]}"; do
        # Извлекаем текущий статус здоровья контейнера средствами Docker API
        health_status=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null)

        # Если у контейнера нет блока healthcheck, проверяем просто факт его работы (running)
        if [ -z "$health_status" ] || [ "$health_status" == "<nil>" ]; then
            is_running=$(docker inspect --format='{{.State.Running}}' "$container" 2>/dev/null)
            if [ "$is_running" != "true" ]; then
                echo "❌ Container $container is dead or not running."
                all_healthy=false
            fi
        elif [ "$health_status" != "healthy" ]; then
            echo "⏳ Container $container is currently: $health_status"
            all_healthy=false
        fi
    done

    if [ "$all_healthy" == "true" ]; then
        echo "💯 SUCCESS: Architecture validation complete. All 6 nodes are 100% HEALTHY!"
        exit 0
    fi

    sleep $SLEEP_INTERVAL
done

echo "💥 CRITICAL VALIDATION FAILURE: Cluster failed to stabilize within 60 seconds."
exit 1
