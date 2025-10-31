#!/bin/bash

# Stop script for data ingestion service
set -e

PROJECT_NAME="data-ingestion-app"

echo "🛑 Stopping services..."
echo ""

# Stop FastAPI service only
echo "🧱 Stopping FastAPI service..."
docker compose -p "$PROJECT_NAME" -f docker-compose.yml down

# Stop Kafka and Kafka UI services only
echo "🧱 Stopping Kafka and Kafka UI services..."
docker compose -p "$PROJECT_NAME" -f docker-compose.kafka.yml down

echo ""
echo "✅ All services stopped and cleaned!"
