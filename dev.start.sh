#!/bin/bash

# Start script for data ingestion service
# Starts both Kafka and FastAPI services together

set -e

PROJECT_NAME="data-ingestion-app"

echo "🚀 Starting Data Ingestion Service..."
echo ""

# Ensure Kafka network exists
echo "🔍 Checking for kafka-net network..."
if ! docker network inspect kafka-net >/dev/null 2>&1; then
  echo "📦 Creating kafka-net network..."
  docker network create kafka-net
else
  echo "✅ Kafka network already exists"
fi

# Start Kafka first
echo ""
echo "📦 Starting Kafka and Kafka UI..."
docker compose -p "$PROJECT_NAME" -f docker-compose.kafka.yml up -d

# Wait for Kafka to be ready
echo ""
echo "⏳ Waiting for Kafka to be ready..."
max_wait=30
wait_time=0

while ! docker exec kafka-local /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:29092 >/dev/null 2>&1; do
  if [ $wait_time -ge $max_wait ]; then
    echo "⚠️  Kafka taking too long to respond, but continuing..."
    break
  fi
  echo "   Waiting for Kafka... (${wait_time}s/${max_wait}s)"
  sleep 2
  wait_time=$((wait_time + 2))
done

if [ $wait_time -lt $max_wait ]; then
  echo "✅ Kafka is ready!"
fi

# Start FastAPI
echo ""
echo "📦 Starting FastAPI ingestion API..."
docker compose -p "$PROJECT_NAME" up -d

echo ""
echo "✅ All services started!"

# Show status
echo "📋 Service Status:"
docker compose -p "$PROJECT_NAME" ps 2>/dev/null || true
# docker compose -p "$PROJECT_NAME" -f docker-compose.kafka.yml ps
