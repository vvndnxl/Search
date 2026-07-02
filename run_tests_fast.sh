#!/usr/bin/env bash
set -e

# Функция полной очистки системы при любом исходе скрипта
cleanup_environment() {
    # 1. Восстанавливаем оригинальный Dockerfile
    if [ -f "Dockerfile.bak" ]; then
        echo "Restoring pristine production Dockerfile structure..."
        mv Dockerfile.bak Dockerfile
    fi
}

# Привязываем функцию очистки к выходу из скрипта
trap cleanup_environment EXIT

# Определение команды docker compose
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE_CMD="docker-compose"
else
    DOCKER_COMPOSE_CMD="docker compose"
fi

# 1. Clean up local metadata compilation caches
echo "Wiping metadata runtime caches..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true

if ! docker-compose ps postgres_db | grep -q "Up"; then
    echo "Error: Core infrastructure containers are not running!"
    echo "Please run ./run.sh first to provision the ecosystem."
    exit 1
fi

# 2. Modify Dockerfile ON THE FLY with sed
cp Dockerfile Dockerfile.bak

# ИНЖЕКЦИЯ 1: Пропуск обновлений apt-get
sed -i 's|RUN apt-get update && apt-get install -y --no-install-recommends \\|RUN echo "Skipping apt-get for fast testing loops" #|g' Dockerfile
sed -i 's| curl \\||g' Dockerfile
sed -i 's| \&\& rm -rf /var/lib/apt/lists/\*||g' Dockerfile

# ИНЖЕКЦИЯ 2: Жесткая установка из локального wheelhouse БЕЗ бэктрекинга
sed -i 's|pip install --no-cache-dir -r requirements.txt|--mount=type=bind,source=wheelhouse,target=/wheelhouse pip install --no-index --find-links=/wheelhouse -r requirements.txt|g' Dockerfile


if grep -q "COPY requirements.txt" Dockerfile; then
    echo "Requirements copy already optimized in Dockerfile."
else
    # Вставляем COPY requirements.txt . прямо перед строкой с нашей кастомной установкой pip
    sed -i '|--mount=type=bind,source=wheelhouse/i COPY requirements.txt .' Dockerfile
fi

# 3. Build only the API target layer strictly without caching
echo " Rebuilding application layers strictly without cache..."
$DOCKER_COMPOSE_CMD build --no-cache api

# 4. Instantly launch the tests inside the isolated container mesh
echo "Running tests stream against container database layout..."
$DOCKER_COMPOSE_CMD run --rm \
  -v "$(pwd)/pytest.ini:/app/pytest.ini" \
  -v "$(pwd)/data.csv:/app/data.csv" \
  api pytest -v tests/

echo "HYPER-FAST TESTS SUITE RUN FINISHED"

