import os
import io
import boto3
import pandas as pd
import logging
from sqlalchemy import create_engine, text


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localstack:4566")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "retail-data-lake")
FILE_KEY = "raw-data/sales.csv"
DB_URL = os.getenv("DATABASE_URL", "postgresql://admin:admin123@postgres:5432/sales_dwh")


s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1"
)

db_engine = create_engine(DB_URL)


def run_pipeline():
    logger.info("🚀 Starting Medallion Pipeline...")

    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
        df_raw = pd.read_csv(io.BytesIO(response['Body'].read()))

        df_raw.to_sql("bronze_sales_raw", db_engine, if_exists="replace", index=False)
        logger.info(f"✅ Bronze loaded: {len(df_raw)} rows")

    except Exception as e:
        logger.error(f"❌ Bronze failed: {e}")
        return

    try:
        df = df_raw.copy()
        df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('-', '_').str.lower()

        df['order_date'] = pd.to_datetime(df['order_date'], format='%d/%m/%Y', errors='coerce')
        df['ship_date'] = pd.to_datetime(df['ship_date'], format='%d/%m/%Y', errors='coerce')
        df['sales'] = pd.to_numeric(df['sales'], errors='coerce')

        df = df.drop_duplicates()
        df = df[df['sales'] > 0]

        df['sales'] = df['sales'].fillna(df['sales'].median())

        df.to_sql("silver_sales_cleaned", db_engine, if_exists="replace", index=False)
        logger.info(f"✅ Silver loaded: {len(df)} rows")

    except Exception as e:
        logger.error(f"❌ Silver failed: {e}")
        return

    try:
        with db_engine.begin() as conn:

            conn.execute(text("""
                DROP TABLE IF EXISTS fact_sales CASCADE;
                DROP TABLE IF EXISTS dim_customer CASCADE;
                DROP TABLE IF EXISTS dim_product CASCADE;
                DROP TABLE IF EXISTS dim_location CASCADE;
                DROP TABLE IF EXISTS dim_date CASCADE;
            """))

    
            conn.execute(text("""
                CREATE TABLE dim_date AS
                SELECT DISTINCT
                    order_date AS date,
                    EXTRACT(YEAR FROM order_date) AS year,
                    EXTRACT(MONTH FROM order_date) AS month,
                    EXTRACT(DAY FROM order_date) AS day
                FROM silver_sales_cleaned;
            """))

            conn.execute(text("""
                ALTER TABLE dim_date ADD COLUMN date_id SERIAL PRIMARY KEY;
            """))

            conn.execute(text("""
                CREATE TABLE dim_customer AS
                SELECT DISTINCT ON (customer_id) customer_id, customer_name, segment
                FROM silver_sales_cleaned
                ORDER BY customer_id;
            """))

            conn.execute(text("""
                ALTER TABLE dim_customer ADD COLUMN customer_key SERIAL PRIMARY KEY;
            """))

        
            conn.execute(text("""
                CREATE TABLE dim_product AS
                SELECT DISTINCT ON (product_id) product_id, product_name, category, sub_category
                FROM silver_sales_cleaned
                ORDER BY product_id;
            """))

            conn.execute(text("""
                ALTER TABLE dim_product ADD COLUMN product_key SERIAL PRIMARY KEY;
            """))

            conn.execute(text("""
                CREATE TABLE dim_location AS
                SELECT DISTINCT ON (postal_code) postal_code, country, city, state, region
                FROM silver_sales_cleaned
                WHERE postal_code IS NOT NULL
                ORDER BY postal_code;
            """))

            conn.execute(text("""
                ALTER TABLE dim_location ADD COLUMN location_key SERIAL PRIMARY KEY;
            """))

            conn.execute(text("""
                CREATE TABLE fact_sales AS
                SELECT 
                    s.row_id,
                    s.order_id,
                    d.date_id,
                    c.customer_key,
                    p.product_key,
                    l.location_key,
                    s.sales
                FROM silver_sales_cleaned s
                LEFT JOIN dim_customer c ON s.customer_id = c.customer_id
                LEFT JOIN dim_product p ON s.product_id = p.product_id
                LEFT JOIN dim_location l ON s.postal_code = l.postal_code
                LEFT JOIN dim_date d ON s.order_date = d.date;
            """))


            conn.execute(text("""
                ALTER TABLE fact_sales ADD PRIMARY KEY (row_id);
            """))


            conn.execute(text("CREATE INDEX idx_fact_date ON fact_sales(date_id);"))
            conn.execute(text("CREATE INDEX idx_fact_customer ON fact_sales(customer_key);"))

        logger.info("🎯 Gold Layer completed with Star Schema + Keys")

    except Exception as e:
        logger.error(f"❌ Gold failed: {e}")

if __name__ == "__main__":
    run_pipeline()