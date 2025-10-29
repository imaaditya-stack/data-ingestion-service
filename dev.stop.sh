#!/bin/bash

# Stop script for data ingestion service

set -e

echo "🛑 Stopping services..."
echo ""

# Stop FastAPI first
echo "Stopping FastAPI app..."
docker compose down

# Stop Kafka
echo "Stopping Kafka and Kafka UI..."
docker compose -f docker-compose.kafka.yml down

echo ""
echo "✅ All services stopped!"