#!/bin/bash

echo "🚀 Starting full system..."

# build + run
docker-compose up -d --build

echo "⏳ Waiting for services to be ready..."
sleep 15

echo "📦 Uploading data to S3..."
docker-compose exec backend python infrastructure/init_s3.py

echo "🔄 Running ETL pipeline..."
docker-compose exec backend python data_pipeline/etl_script.py

echo "✅ System is ready!"
echo "👉 UI: http://localhost:8501"
echo "👉 API: http://localhost:8000"