#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "================================================================="
echo "🔄 RUNNING SYSTEM REBOOT, CACHE PURGE & AUTOMATED TEST PIPELINE"
echo "================================================================="

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
$DOCKER_COMPOSE_CMD up -d postgres_db elasticsearch kafka

# 4. Wait for core system health checks to finalize database readiness
echo "Step 5: Awaiting database and full-text search index readiness..."
until $DOCKER_COMPOSE_CMD ps postgres_db | grep -q "healthy"; do
    sleep 2
    echo -n "."
done
until $DOCKER_COMPOSE_CMD ps elasticsearch | grep -q "healthy"; do
    sleep 2
    echo -n "."
done
echo ""

# 5. AUTOMATED INTEGRATION TESTING STAGE
echo "Step 6: Provisioning isolated test environments and running test cases..."
# Generate the mandatory 'test_search_db' inside the running PostgreSQL container
$DOCKER_COMPOSE_CMD exec -T postgres_db psql -U postgres -d postgres -c "CREATE DATABASE test_search_db;" || true

# Run the test suite inside a temporary, isolated API instance
# By using '--rm', the test environment container is automatically destroyed afterward
$DOCKER_COMPOSE_CMD run --rm \
  -v "$(pwd)/pytest.ini:/app/pytest.ini" \
  -v "$(pwd)/data.csv:/app/data.csv" \
  api pytest -v tests/


# 6. Complete primary application container orchestration
echo "🚀 Step 7: Launching primary API server and background synchronization worker..."
$DOCKER_COMPOSE_CMD up -d api worker

echo ""
echo " DEPLOYMENT FINALIZED SUCCESSFULLY"
echo ""
echo "API Engine Online:   http://localhost:8000/docs"
echo "🔍 Elasticsearch Node:  http://localhost:9200"
echo ""
echo "Next steps:"
echo "   1) Run 'python -m app.generate_openapi' to extract your 'docs.json' contract."
echo "   2) Run 'python -m app.etl_pipeline' to chunk-stream your source 'data.csv'."
echo ""
