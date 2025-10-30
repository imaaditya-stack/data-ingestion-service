#!/bin/bash

# Stop script for data ingestion service
set -e

PROJECT_NAME="ingestion_stack"

echo "🛑 Stopping services..."
echo ""

# Stop FastAPI app only
echo "🧱 Stopping FastAPI app..."
docker compose -p "$PROJECT_NAME" -f docker-compose.yml down

# Stop Kafka and Kafka UI
echo "🧱 Stopping Kafka and Kafka UI..."
docker compose -p "$PROJECT_NAME" -f docker-compose.kafka.yml down

echo ""
echo "✅ All services stopped and cleaned!"
