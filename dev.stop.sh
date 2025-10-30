#!/bin/bash

# Stop script for data ingestion service
set -e

PROJECT_NAME="ingestion_stack"

echo "🛑 Stopping services..."
echo ""

# Stop FastAPI first
echo "🧱 Stopping FastAPI app..."
docker compose -p "$PROJECT_NAME" down --remove-orphans

# Stop Kafka
echo "🧱 Stopping Kafka and Kafka UI..."
docker compose -p "$PROJECT_NAME" -f docker-compose.kafka.yml down --remove-orphans

echo ""
echo "✅ All services stopped and cleaned!"
