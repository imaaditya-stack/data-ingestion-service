#!/bin/bash

# Start script for data ingestion service
# Starts both Kafka and FastAPI together

set -e

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
docker compose -f docker-compose.kafka.yml up -d

# Wait for Kafka to be ready
echo ""
echo "⏳ Waiting for Kafka to be ready..."
max_wait=30
wait_time=0

# Try to connect to Kafka port
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
docker compose up -d

echo ""
echo "✅ All services started!"
echo ""
echo "📊 Services:"
echo "  - Kafka: localhost:9092"
echo "  - Kafka UI: http://localhost:8080"
echo "  - API: http://localhost:8000"
echo "  - Health: http://localhost:8000/health"
echo ""

# Show status
echo "📋 Service Status:"
docker compose ps 2>/dev/null || true
docker compose -f docker-compose.kafka.yml ps

echo ""
echo "📋 Useful commands:"
echo "  - View logs: docker compose logs -f ingestion-api"
echo "  - View Kafka logs: docker compose -f docker-compose.kafka.yml logs -f"
echo "  - Stop: ./stop.sh"