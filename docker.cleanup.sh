#!/bin/bash
# 🚨 Emergency Docker Cleanup Script
# Safely stops and removes all containers, networks, and resources for the AI Platform.

set -e

# Load .env variables
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
else
  echo "❌ .env file not found!"
  exit 1
fi

echo "🧹 Starting full cleanup for project: $PROJECT_NAME"
echo ""

# 1️⃣ Stop all AI Platform services gracefully
echo "🛑 Stopping running Compose services..."
docker compose -p "$PROJECT_NAME" -f docker-compose.yml down -v --remove-orphans || true
docker compose -p "$PROJECT_NAME" -f docker-compose.kafka.yml down -v --remove-orphans || true

# 2️⃣ Remove custom Docker network (if exists)
if docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
  echo "🔌 Removing Docker network: $NETWORK_NAME"
  docker network rm "$NETWORK_NAME" || true
else
  echo "✅ Network $NETWORK_NAME already removed or doesn't exist"
fi

# 3️⃣ Clean up dangling containers, images, and volumes
echo ""
echo "🧽 Removing all dangling containers, images, and volumes..."
docker system prune -af --volumes

# 4️⃣ Optionally remove local images built for this project
echo ""
echo "🧱 Removing local images for $PROJECT_NAME (if any)..."
PROJECT_IMAGES=$(docker images | grep "$PROJECT_NAME" | awk '{print $3}')
if [ -n "$PROJECT_IMAGES" ]; then
  echo "🧨 Found images for $PROJECT_NAME, removing..."
  docker rmi -f $PROJECT_IMAGES || true
else
  echo "✅ No local images found for $PROJECT_NAME"
fi

echo ""
echo "✅ Cleanup complete! You now have a clean Docker environment."
