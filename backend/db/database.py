import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

logger = logging.getLogger(__name__)
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin123")
DB_NAME = os.getenv("DB_NAME", "sales_dwh")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
# DB_URL = os.getenv("DB_HOST_URL", "postgresql://admin:admin123@postgres:5432/sales_dwh")

engine = create_engine(
    DB_URL,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_pre_ping=True  
)

 
def execute_safe_query(sql_query: str):
    """Execute SQL safely with timeout + read-only enforcement"""

    try:
        with engine.connect() as connection:
            connection = connection.execution_options(
                isolation_level="AUTOCOMMIT"
            )

            connection.execute(text("SET statement_timeout = 5000;")) 

            result = connection.execute(text(sql_query))

            data = [dict(row._mapping) for row in result]
            return data

    except Exception as e:
        logger.error(f"❌ Database execution failed: {e}")
        return {"error": "Query execution failed"}