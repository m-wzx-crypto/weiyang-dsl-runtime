#!/bin/bash
set -e

REMOTE_DIR="${REMOTE_DIR:-/opt/app}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"

cd "$REMOTE_DIR"

echo "=== ModuMind Production Deploy ==="
echo "Directory:  $REMOTE_DIR"
echo "Compose:    $COMPOSE_FILE"
echo "Timestamp:  $(date -Iseconds)"
echo ""

echo "Pulling latest images..."
docker compose -f "$COMPOSE_FILE" pull

echo ""
echo "Running database migrations..."
docker compose -f "$COMPOSE_FILE" run --rm migrate

echo ""
echo "Starting services..."
docker compose -f "$COMPOSE_FILE" up -d

echo ""
echo "Waiting for services to stabilize..."
sleep 10

echo ""
echo "=== Service Status ==="
docker compose -f "$COMPOSE_FILE" ps

echo ""
echo "=== Health Checks ==="
for svc in gateway postgres redis; do
    HEALTH=$(docker compose -f "$COMPOSE_FILE" ps "$svc" --format json 2>/dev/null | grep -o '"Health":"[^"]*"' | head -1 || echo "unknown")
    echo "  $svc: $HEALTH"
done

echo ""
echo "Cleaning up old images..."
docker image prune -f --filter "until=168h"

echo ""
echo "=== Deployment completed ==="
echo "Finished at: $(date -Iseconds)"
