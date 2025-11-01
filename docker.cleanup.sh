#!/bin/bash
# 🚨 Emergency Docker Cleanup Script
# Safely stops all project containers and cleans up everything related.

set -e

PROJECT_NAME="data-ingestion-app"
NETWORK_NAME="kafka-net"

echo "🧹 Starting full cleanup for project: $PROJECT_NAME"
echo ""

# 1️⃣ Stop all services gracefully
echo "🛑 Stopping running Compose services..."
docker compose -p "$PROJECT_NAME" -f docker-compose.yml down -v --remove-orphans || true
docker compose -p "$PROJECT_NAME" -f docker-compose.kafka.yml down -v --remove-orphans || true

# 2️⃣ Remove custom Docker network
if docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
  echo "🔌 Removing Docker network: $NETWORK_NAME"
  docker network rm "$NETWORK_NAME" || true
fi

# 3️⃣ Remove old containers and dangling images
echo ""
echo "🧽 Removing all dangling containers, images, and volumes..."
docker system prune -af --volumes

# 4️⃣ Remove old build cache for this project (optional but safe)
echo ""
echo "🧱 Removing local image for $PROJECT_NAME (if exists)..."
docker images | grep "$PROJECT_NAME" && docker rmi $(docker images "$PROJECT_NAME" -q) || true

echo ""
echo "✅ Cleanup complete! You now have a clean Docker environment."