#!/bin/bash

# Stop script for AI Platform (Backend, Consumer, Kafka, Vector DB)
set -e

# Load .env variables
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
else
  echo "❌ .env file not found!"
  exit 1
fi

echo "🛑 Stopping AI Platform Services..."
echo ""

# Stop API, Consumer, and Vector DB
echo "🧱 Stopping AI Backend, Kafka Consumer, and Vector Database..."
docker compose -p "$PROJECT_NAME" -f docker-compose.yml down

# Stop Kafka stack separately
echo ""
echo "🧱 Stopping Kafka and Kafka UI..."
docker compose -p "$PROJECT_NAME" -f docker-compose.kafka.yml down

echo ""
echo "✅ All services stopped and cleaned!"
