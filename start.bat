@echo off


echo 🚀 Building and starting all services...
docker-compose up -d --build
if %ERRORLEVEL% NEQ 0 (
    echo ❌ docker-compose up failed.
    pause
    exit /b 1
)

:wait_postgres
echo ⏳ Waiting for Postgres...
docker-compose exec -T postgres pg_isready -U admin -d sales_dwh >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    timeout /t 2 /nobreak >nul
    goto wait_postgres
)
echo ✅ Postgres is ready.

:wait_localstack
echo ⏳ Waiting for LocalStack S3...
curl -sf "http://localhost:4566/_localstack/health" | findstr /C:"running" >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    timeout /t 2 /nobreak >nul
    goto wait_localstack
)
echo ✅ LocalStack S3 is ready.

:wait_etl
echo ⏳ Waiting for ETL pipeline to complete...
for /f "tokens=*" %%i in ('docker inspect -f "{{.State.Status}}" etl_runner 2^>nul') do set STATUS=%%i
if not "%STATUS%"=="exited" (
    timeout /t 3 /nobreak >nul
    goto wait_etl
)

for /f "tokens=*" %%i in ('docker inspect -f "{{.State.ExitCode}}" etl_runner 2^>nul') do set EXIT_CODE=%%i
if not "%EXIT_CODE%"=="0" (
    echo ❌ ETL pipeline failed ^(exit code %EXIT_CODE%^). Check logs:
    echo    docker-compose logs etl_runner
    pause
    exit /b 1
)
echo ✅ ETL pipeline completed successfully.

echo.
echo 🎉 System is ready!
echo    👉 UI  : http://localhost:8501
echo    👉 API : http://localhost:8000/docs
pause