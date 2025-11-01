#!/bin/bash
# 🚀 Rebuild + Start Script for Data Ingestion Service

set -e

PROJECT_NAME="data-ingestion-app"

echo "🔧 Starting rebuild process for $PROJECT_NAME..."
echo ""

# 1️⃣ Ensure everything old is stopped first (optional safety)
echo "🛑 Ensuring old services are stopped..."
docker compose -p "$PROJECT_NAME" -f docker-compose.yml down --remove-orphans || true

#Comment out if you don't want to rebuild the Kafka services
# docker compose -p "$PROJECT_NAME" -f docker-compose.kafka.yml down --remove-orphans || true

# 2️⃣ Clean rebuild (forces Docker to rebuild all layers)
echo ""
echo "🧱 Rebuilding Docker images from scratch..."
docker compose -p "$PROJECT_NAME" build --no-cache

# 3️⃣ Start all services via existing start script
echo ""
echo "🚀 Launching all services..."
./dev.start.sh

echo ""
echo "✅ Rebuild and startup complete!"
docker compose -p "$PROJECT_NAME" ps