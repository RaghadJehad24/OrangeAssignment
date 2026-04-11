#!/bin/bash


set -euo pipefail


wait_for_postgres() {
  echo "⏳ Waiting for Postgres to be ready..."
  until docker-compose exec -T postgres \
        pg_isready -U "${DB_USER:-admin}" -d "${DB_NAME:-sales_dwh}" \
        > /dev/null 2>&1; do
    printf "."
    sleep 2
  done
  echo ""
  echo "✅ Postgres is ready."
}

wait_for_localstack() {
  echo "⏳ Waiting for LocalStack S3 to be ready..."
  until curl -sf "http://localhost:4566/_localstack/health" \
        | grep -q '"s3": "running"' > /dev/null 2>&1; do
    printf "."
    sleep 2
  done
  echo ""
  echo "✅ LocalStack S3 is ready."
}

wait_for_etl() {
  echo "⏳ Waiting for ETL pipeline to complete..."
  until [ "$(docker inspect -f '{{.State.Status}}' etl_runner 2>/dev/null)" = "exited" ]; do
    printf "."
    sleep 3
  done
  echo ""

  EXIT_CODE=$(docker inspect -f '{{.State.ExitCode}}' etl_runner)
  if [ "$EXIT_CODE" != "0" ]; then
    echo "❌ ETL pipeline failed (exit code $EXIT_CODE). Check logs:"
    echo "   docker-compose logs etl_runner"
    exit 1
  fi
  echo "✅ ETL pipeline completed successfully."
}


echo "🚀 Building and starting all services..."
docker-compose up -d --build

wait_for_postgres
wait_for_localstack
wait_for_etl

echo ""
echo "🎉 System is ready!"
echo "   👉 UI  : http://localhost:8501"
echo "   👉 API : http://localhost:8000/docs"