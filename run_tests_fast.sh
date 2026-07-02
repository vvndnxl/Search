#!/usr/bin/env bash

set -e

echo "================================================================="
echo "⚡ FAST DEV RUNNER: HYPER-OPTIMIZED OFFLINE INJECTION PIPELINE"
echo "================================================================="

# Function to restore original Dockerfile upon any script outcome
cleanup_dockerfile() {
    if [ -f "Dockerfile.bak" ]; then
        echo "♻️ Restoring pristine production Dockerfile structure..."
        mv Dockerfile.bak Dockerfile
    fi
}
trap cleanup_dockerfile EXIT

if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE_CMD="docker-compose"
else
    DOCKER_COMPOSE_CMD="docker compose"
fi

# 1. Clean up local metadata compilation caches
echo "🧹 Wiping metadata runtime caches..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true

# 2. Modify Dockerfile ON THE FLY with sed
echo "🔧 Dynamically injecting hyper-fast offline configurations..."
cp Dockerfile Dockerfile.bak

# INJECTION 1: Skip apt-get and curl update routines for testing speed
sed -i 's|RUN apt-get update && apt-get install -y --no-install-recommends \\|RUN echo "Skipping apt-get for fast testing loops" #|g' Dockerfile
sed -i 's|    curl \\||g' Dockerfile
sed -i 's|    \&\& rm -rf /var/lib/apt/lists/\*||g' Dockerfile

# INJECTION 2 (FIXED): Force pip to install directly from local wheels, allowing full dependency resolution locally
sed -i 's|pip install --no-cache-dir -r requirements.txt|--mount=type=bind,source=wheelhouse,target=/wheelhouse PIP_FIND_LINKS=/wheelhouse PIP_NO_INDEX=true pip install -r requirements.txt --prefer-binary|g' Dockerfile

# 3. Build only the API target layer strictly without caching
echo "🏗️  Rebuilding application layers strictly without cache..."
$DOCKER_COMPOSE_CMD build --no-cache api

# 4. Instantly launch the tests inside the isolated container mesh
echo "🧪 Running tests stream against container database layout..."
$DOCKER_COMPOSE_CMD run --rm \
  -v "$(pwd)/pytest.ini:/app/pytest.ini" \
  -v "$(pwd)/data.csv:/app/data.csv" \
  api pytest -v tests/

echo "================================================================="
echo "🎉 HYPER-FAST TESTS SUITE RUN FINISHED"
echo "================================================================="
