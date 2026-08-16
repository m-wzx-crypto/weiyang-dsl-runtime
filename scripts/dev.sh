#!/bin/bash
# 启动基座基础设施（postgres/redis/qdrant/minio）并等待健康。
set -e

echo "=== Infrastructure Setup ==="

if [ ! -f .env ]; then
    echo "Copying .env.example to .env..."
    cp .env.example .env
    echo "WARNING: Please review and update .env with your actual configuration values."
else
    echo ".env file already exists, skipping copy."
fi

echo ""
echo "Starting infrastructure services (postgres, redis, qdrant, minio)..."
docker compose -f docker-compose.base.yml up -d

echo ""
echo "Waiting for infrastructure services to be healthy..."
MAX_WAIT=60
ELAPSED=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
    PG_HEALTHY=$(docker inspect --format='{{.State.Health.Status}}' modumind-postgres 2>/dev/null || echo "unknown")
    REDIS_HEALTHY=$(docker inspect --format='{{.State.Health.Status}}' modumind-redis 2>/dev/null || echo "unknown")
    QDRANT_HEALTHY=$(docker inspect --format='{{.State.Health.Status}}' modumind-qdrant 2>/dev/null || echo "unknown")
    MINIO_HEALTHY=$(docker inspect --format='{{.State.Health.Status}}' modumind-minio 2>/dev/null || echo "unknown")

    if [ "$PG_HEALTHY" = "healthy" ] && [ "$REDIS_HEALTHY" = "healthy" ] && \
       [ "$QDRANT_HEALTHY" = "healthy" ] && [ "$MINIO_HEALTHY" = "healthy" ]; then
        echo "All infrastructure services are healthy"
        break
    fi

    echo "  Waiting... postgres=$PG_HEALTHY redis=$REDIS_HEALTHY qdrant=$QDRANT_HEALTHY minio=$MINIO_HEALTHY (${ELAPSED}s/${MAX_WAIT}s)"
    sleep 2
    ELAPSED=$((ELAPSED + 2))
done

if [ $ELAPSED -ge $MAX_WAIT ]; then
    echo "WARNING: Infrastructure services did not become healthy within ${MAX_WAIT}s, proceeding anyway..."
fi

echo ""
echo "=== Infrastructure Ready ==="
echo "PostgreSQL:  localhost:${POSTGRES_PORT:-5432}"
echo "Redis:       localhost:${REDIS_PORT:-6379}"
echo "Qdrant:      localhost:${QDRANT_PORT:-6333}"
echo "MinIO API:   localhost:${MINIO_PORT:-9000}"
echo "MinIO UI:    localhost:${MINIO_CONSOLE_PORT:-9001}"
echo ""
echo "监控栈（Prometheus/Grafana）: docker compose -f monitoring/docker-compose.monitoring.yml up -d"
