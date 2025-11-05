#!/bin/bash

# Start script for AI Backend and Data Ingestion Consumer
# Spins up Kafka (if not running), Vector DB, API, and Kafka Consumer

set -e

# Load .env variables
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
else
  echo "❌ .env file not found!"
  exit 1
fi

echo "🚀 Starting AI Platform Services..."
echo ""

# Ensure Kafka network exists
echo "🔍 Checking for $NETWORK_NAME network..."
if ! docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
  echo "📦 Creating $NETWORK_NAME network..."
  docker network create "$NETWORK_NAME"
else
  echo "✅ $NETWORK_NAME network already exists"
fi

# Start Kafka stack first (defined in docker-compose.kafka.yml)
echo ""
echo "📦 Starting Kafka & Kafka UI..."
docker compose -p "$PROJECT_NAME" -f docker-compose.kafka.yml up -d

# Wait for Kafka to be ready
echo ""
echo "⏳ Waiting for Kafka to be ready..."
max_wait=30
wait_time=0

while ! docker exec kafka-local /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka:29092 >/dev/null 2>&1; do
  if [ $wait_time -ge $max_wait ]; then
    echo "⚠️  Kafka is taking too long, continuing..."
    break
  fi
  echo "   Waiting for Kafka... (${wait_time}s/${max_wait}s)"
  sleep 2
  wait_time=$((wait_time + 2))
done

if [ $wait_time -lt $max_wait ]; then
  echo "✅ Kafka is ready!"
fi

# Start vector DB, API, and consumer containers
echo ""
echo "📦 Starting AI Backend, Vector DB & Kafka Consumer..."
docker compose -p "$PROJECT_NAME" up -d 

echo ""
echo "✅ All services started successfully!"
echo ""

# Show status
echo "📋 Service Status:"
docker compose -p "$PROJECT_NAME" ps
