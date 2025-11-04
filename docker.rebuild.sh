#!/bin/bash
# 🚀 Rebuild + Start Script for AI Platform

set -e

# Load .env variables
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
else
  echo "❌ .env file not found!"
  exit 1
fi

echo "🔧 Starting rebuild process for $PROJECT_NAME..."
echo ""

# 1️⃣ Ensure existing containers are stopped
echo "🛑 Ensuring old services are stopped..."
docker compose -p "$PROJECT_NAME" -f docker-compose.yml down --remove-orphans || true

# Uncomment below if you want to rebuild Kafka stack as well
# echo ""
# echo "🛑 Stopping Kafka and Kafka UI..."
# docker compose -p "$PROJECT_NAME" -f docker-compose.kafka.yml down --remove-orphans || true

# 2️⃣ Clean rebuild (forces Docker to rebuild all layers)
echo ""
echo "🧱 Rebuilding Docker images from scratch..."
docker compose -p "$PROJECT_NAME" build --no-cache

# 3️⃣ Start all services via your dev start script
echo ""
echo "🚀 Launching all services..."
./dev.start.sh

echo ""
echo "✅ Rebuild and startup complete!"
docker compose -p "$PROJECT_NAME" ps
