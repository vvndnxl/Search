#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "RUNNING SYSTEM REBOOT, CACHE PURGE & AUTOMATED TEST PIPELINE"

# Dynamic command checker mapping 'docker-compose' to modern 'docker compose' if required
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE_CMD="docker-compose"
else
    DOCKER_COMPOSE_CMD="docker compose"
fi

echo "[Environment] Utilizing command wrapper: $DOCKER_COMPOSE_CMD"

# 1. Gracefully teardown running infrastructure and remove volumes
if [ -f "docker-compose.yml" ]; then
    echo "Step 1: Stopping infrastructure and purging active volumes..."
    $DOCKER_COMPOSE_CMD down -v --remove-orphans || true
else
    echo "Warning: docker-compose.yml not found. Skipping down phase."
fi

# 2. Hard-purge leftover python artifacts and execution caches
echo "Step 2: Obliterating Python cache registries globally..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true
find . -type f -name "*.pyo" -delete 2>/dev/null || true
find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true

# 3. Boot database infrastructure up first to provision environments
echo "Step 3: Compiling un-cached container configurations..."
$DOCKER_COMPOSE_CMD build --no-cache

echo "Step 4: Spawning base infrastructure containers..."
$DOCKER_COMPOSE_CMD up -d postgres_db elasticsearch kafka redis

# 4. Wait for core system health checks to finalize database readiness using native docker inspect
echo "Step 5: Awaiting database, cache and full-text search index readiness..."
CHECK_SERVICES=("search_postgres" "search_elastic" "search_kafka" "search_redis")
MAX_ATTEMPTS=30

for service in "${CHECK_SERVICES[@]}"; do
    echo -n "Awaiting health status for $service ."
    attempt=1
    while [ $attempt -le $MAX_ATTEMPTS ]; do
        health_status=$(docker inspect --format='{{.State.Health.Status}}' "$service" 2>/dev/null || echo "starting")
        if [ "$health_status" == "healthy" ]; then
            echo " [OK]"
            break
        fi
        sleep 2
        echo -n "."
        attempt=$((attempt+1))
    done
    if [ $attempt -gt $MAX_ATTEMPTS ]; then
        echo -e "\n❌ CRITICAL: Service $service failed to become healthy in time!"
        exit 1
    fi
done

# 5. AUTOMATED INTEGRATION TESTING STAGE
echo "Step 6: Provisioning isolated test environments and running test cases..."
$DOCKER_COMPOSE_CMD exec -T postgres_db psql -U postgres -d postgres -c "CREATE DATABASE test_search_db;" || true

$DOCKER_COMPOSE_CMD run --rm \
  -v "$(pwd)/pytest.ini:/app/pytest.ini" \
  -v "$(pwd)/tests/test_data.csv:/app/tests/test_data.csv" \
  api pytest -v tests/

# 6. Complete primary application container orchestration
echo "🚀 Step 7: Launching primary API server and background synchronization worker..."
$DOCKER_COMPOSE_CMD up -d api worker

echo "Step 8: Performing final cluster validation inspection..."
FINAL_SERVICES=("search_api" "search_worker")

for service in "${FINAL_SERVICES[@]}"; do
    echo -n "Verifying startup stability for $service ."
    # API и воркер запускаются быстро, даем 5 попыток
    for attempt in {1..5}; do
        is_running=$(docker inspect --format='{{.State.Running}}' "$service" 2>/dev/null || echo "false")
        if [ "$is_running" == "true" ]; then
            echo " [ONLINE]"
            break
        fi
        sleep 2
        echo -n "."
        if [ $attempt -eq 5 ]; then
            echo -e "\n❌ CRITICAL CRASH DETECTION: $service failed to maintain runtime stability!"
            docker logs "$service" || true
            exit 1
        fi
    done
done

echo ""
echo " DEPLOYMENT FINALIZED SUCCESSFULLY"
echo "API Engine Online:      http://localhost:8000/docs"
echo "Elasticsearch Node:   http://localhost:9200"
echo "Next steps:"
echo "   1) Run 'python -m app.generate_openapi' to extract your 'docs.json' contract."
echo "   2) Run 'python -m app.etl_pipeline' to chunk-stream your source 'data.csv'."
echo ""
