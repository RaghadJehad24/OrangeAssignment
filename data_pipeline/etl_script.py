import os
import io
import uuid
import hashlib
import json
import logging
from datetime import datetime

import boto3
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

S3_ENDPOINT  = os.getenv("S3_ENDPOINT",   "http://localstack:4566")
BUCKET_NAME  = os.getenv("S3_BUCKET_NAME","retail-data-lake")
S3_PREFIX    = "raw-data/"
DB_URL       = os.getenv("DATABASE_URL",  "postgresql://admin:admin123@postgres:5432/sales_dwh")

s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)
db_engine = create_engine(DB_URL)



def setup_all_tables(conn) -> None:
    """Create every control, staging, and warehouse table if it doesn't exist."""

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS etl_run_log (
            run_id        VARCHAR(36)  PRIMARY KEY,
            pipeline_name VARCHAR(100),
            start_time    TIMESTAMP,
            end_time      TIMESTAMP,
            status        VARCHAR(20),
            error_message TEXT
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS etl_step_log (
            id            SERIAL       PRIMARY KEY,
            run_id        VARCHAR(36),
            step_name     VARCHAR(100),
            source_rows   INT,
            target_rows   INT,
            rejected_rows INT,
            logged_at     TIMESTAMP    DEFAULT NOW()
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS etl_file_registry (
            id            SERIAL        PRIMARY KEY,
            file_key      VARCHAR(500)  UNIQUE,
            file_checksum VARCHAR(64),
            row_count     INT,
            batch_id      VARCHAR(36),
            loaded_at     TIMESTAMP     DEFAULT NOW(),
            status        VARCHAR(20)
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS bronze_sales_raw (
            id           SERIAL      PRIMARY KEY,
            source_file  VARCHAR(500),
            batch_id     VARCHAR(36),
            ingested_at  TIMESTAMP   DEFAULT NOW(),
            raw_data     JSONB
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS silver_sales_cleaned (
            row_id        VARCHAR(50)   PRIMARY KEY,   -- business PK
            order_id      VARCHAR(50),
            order_date    DATE,
            ship_date     DATE,
            ship_mode     VARCHAR(50),
            customer_id   VARCHAR(50),
            customer_name VARCHAR(200),
            segment       VARCHAR(50),
            country       VARCHAR(100),
            city          VARCHAR(100),
            state         VARCHAR(100),
            postal_code   VARCHAR(20),
            region        VARCHAR(100),
            product_id    VARCHAR(50),
            category      VARCHAR(100),
            sub_category  VARCHAR(100),
            product_name  VARCHAR(500),
            sales         NUMERIC(12,4),
            location_bk   VARCHAR(64),   -- MD5(postal|city|state|country)
            batch_id      VARCHAR(36),
            source_file   VARCHAR(500),
            ingested_at   TIMESTAMP     DEFAULT NOW(),
            updated_at    TIMESTAMP     DEFAULT NOW()
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS silver_sales_rejected (
            id               SERIAL   PRIMARY KEY,
            raw_row          JSONB,
            rejection_reason TEXT,
            batch_id         VARCHAR(36),
            rejected_at      TIMESTAMP DEFAULT NOW()
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id SERIAL PRIMARY KEY,
            date    DATE   UNIQUE NOT NULL,
            year    INT,
            month   INT,
            day     INT
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_key  SERIAL       PRIMARY KEY,
            customer_id   VARCHAR(50)  UNIQUE NOT NULL,   -- business key
            customer_name VARCHAR(200),
            segment       VARCHAR(50),
            updated_at    TIMESTAMP    DEFAULT NOW()
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_product (
            product_key  SERIAL       PRIMARY KEY,
            product_id   VARCHAR(50)  UNIQUE NOT NULL,    -- business key
            product_name VARCHAR(500),
            category     VARCHAR(100),
            sub_category VARCHAR(100),
            updated_at   TIMESTAMP    DEFAULT NOW()
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_location (
            location_key SERIAL       PRIMARY KEY,
            location_bk  VARCHAR(64)  UNIQUE NOT NULL,    -- MD5 hash BK
            postal_code  VARCHAR(20),
            country      VARCHAR(100),
            city         VARCHAR(100),
            state        VARCHAR(100),
            region       VARCHAR(100),
            updated_at   TIMESTAMP    DEFAULT NOW()
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS fact_sales (
            row_id        VARCHAR(50)  PRIMARY KEY,
            order_id      VARCHAR(50),
            date_id       INT          REFERENCES dim_date(date_id),
            customer_key  INT          REFERENCES dim_customer(customer_key),
            product_key   INT          REFERENCES dim_product(product_key),
            location_key  INT          REFERENCES dim_location(location_key),
            sales         NUMERIC(12,4),
            batch_id      VARCHAR(36),
            source_file   VARCHAR(500),
            ingested_at   TIMESTAMP    DEFAULT NOW()
        );
    """))

    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_date       ON fact_sales(date_id);"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_customer   ON fact_sales(customer_key);"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_product    ON fact_sales(product_key);"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fact_location   ON fact_sales(location_key);"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_silver_customer ON silver_sales_cleaned(customer_id);"))


def make_run_id() -> str:
    return str(uuid.uuid4())

def md5_checksum(content: bytes) -> str:
    return hashlib.md5(content).hexdigest()

def make_location_bk(row: pd.Series) -> str:
    """
    Stronger location business key — MD5 of postal|city|state|country.
    Prevents two cities sharing a postal code from colliding.
    """
    parts = "|".join([
        str(row.get("postal_code", "") or "").strip().upper(),
        str(row.get("city",        "") or "").strip().upper(),
        str(row.get("state",       "") or "").strip().upper(),
        str(row.get("country",     "") or "").strip().upper(),
    ])
    return hashlib.md5(parts.encode()).hexdigest()



def log_run(conn, run_id, name, start, end, status, err=None):
    conn.execute(text("""
        INSERT INTO etl_run_log (run_id, pipeline_name, start_time, end_time, status, error_message)
        VALUES (:rid, :pn, :st, :et, :s, :em)
        ON CONFLICT (run_id) DO UPDATE
            SET end_time=EXCLUDED.end_time, status=EXCLUDED.status, error_message=EXCLUDED.error_message
    """), {"rid": run_id, "pn": name, "st": start, "et": end, "s": status, "em": err})


def log_step(conn, run_id, step, src, tgt, rej):
    conn.execute(text("""
        INSERT INTO etl_step_log (run_id, step_name, source_rows, target_rows, rejected_rows)
        VALUES (:rid, :sn, :sr, :tr, :rr)
    """), {"rid": run_id, "sn": step, "sr": src, "tr": tgt, "rr": rej})


def is_file_processed(conn, file_key: str, checksum: str) -> bool:
    row = conn.execute(text("""
        SELECT 1 FROM etl_file_registry
        WHERE file_key = :fk AND file_checksum = :cs AND status = 'SUCCESS'
    """), {"fk": file_key, "cs": checksum}).fetchone()
    return row is not None


def register_file(conn, file_key, checksum, row_count, batch_id, status):
    conn.execute(text("""
        INSERT INTO etl_file_registry (file_key, file_checksum, row_count, batch_id, status)
        VALUES (:fk, :cs, :rc, :bid, :st)
        ON CONFLICT (file_key) DO UPDATE
            SET file_checksum=EXCLUDED.file_checksum, row_count=EXCLUDED.row_count,
                batch_id=EXCLUDED.batch_id, status=EXCLUDED.status, loaded_at=NOW()
    """), {"fk": file_key, "cs": checksum, "rc": row_count, "bid": batch_id, "st": status})




REQUIRED_BUSINESS_KEYS = ["row_id", "order_id", "customer_id", "product_id"]

def validate_and_clean(df_raw: pd.DataFrame, batch_id: str, source_file: str):
    """
    Apply column-level DQ rules.
    Returns:
        df_clean   — rows that passed all checks (with location_bk added)
        rejected   — list of dicts {raw_row, reason} for reject table
    """
    df = df_raw.copy()

    df.columns = (
        df.columns.str.strip()
        .str.replace(" ", "_")
        .str.replace("-", "_")
        .str.lower()
    )

    rejected = []
    valid_mask = pd.Series([True] * len(df), index=df.index)

    for col in REQUIRED_BUSINESS_KEYS:
        if col in df.columns:
            null_rows = df.index[df[col].isnull() & valid_mask]
            for idx in null_rows:
                rejected.append({
                    "raw_row": df.loc[idx].to_dict(),
                    "reason":  f"NULL business key: {col}",
                })
            valid_mask &= df[col].notnull()

    df["order_date"] = pd.to_datetime(df["order_date"], format="%d/%m/%Y", errors="coerce")
    df["ship_date"]  = pd.to_datetime(df["ship_date"],  format="%d/%m/%Y", errors="coerce")

    bad_date = df["order_date"].isnull() & valid_mask
    for idx in df.index[bad_date]:
        rejected.append({"raw_row": df.loc[idx].to_dict(), "reason": "Unparseable order_date"})
    valid_mask &= ~bad_date

    df["sales"] = pd.to_numeric(df["sales"], errors="coerce")

    null_sales = df["sales"].isnull() & valid_mask
    for idx in df.index[null_sales]:
        rejected.append({"raw_row": df.loc[idx].to_dict(), "reason": "NULL / non-numeric sales"})
    valid_mask &= ~null_sales

    
    neg_sales = (df["sales"] <= 0) & valid_mask
    for idx in df.index[neg_sales]:
        rejected.append({
            "raw_row": df.loc[idx].to_dict(),
            "reason":  f"Sales <= 0: {df.loc[idx, 'sales']}",
        })
    valid_mask &= ~neg_sales

    df_valid = df[valid_mask].drop_duplicates(subset=["row_id"], keep="last").copy()

    df_valid["location_bk"] = df_valid.apply(make_location_bk, axis=1)
    df_valid["batch_id"]    = batch_id
    df_valid["source_file"] = source_file

    logger.info(
        f"DQ: {len(df_raw)} in → {len(df_valid)} valid | {len(rejected)} rejected"
    )
    return df_valid, rejected


def load_bronze(df_raw: pd.DataFrame, batch_id: str, file_key: str, run_id: str):
    """Append raw rows to bronze — immutable landing zone."""
    df_b = df_raw.copy()
    df_b["_batch_id"]    = batch_id
    df_b["_source_file"] = file_key
    df_b["_ingested_at"] = datetime.utcnow()

    df_b.to_sql("bronze_sales_raw_staging", db_engine, if_exists="replace", index=False)

    with db_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO bronze_sales_raw (source_file, batch_id, ingested_at, raw_data)
            SELECT _source_file, _batch_id, _ingested_at::timestamp,
                   row_to_json(s)::jsonb
            FROM bronze_sales_raw_staging s
        """))
        log_step(conn, run_id, "bronze", len(df_raw), len(df_raw), 0)

    logger.info(f"✅ Bronze: {len(df_raw)} rows appended")


def load_silver(df_clean: pd.DataFrame, rejected: list, batch_id: str, run_id: str):
    """
    Upsert clean rows into silver via staging table.
    Send rejected rows to silver_sales_rejected.
    """
  
    if rejected:
        with db_engine.begin() as conn:
            for rej in rejected:
                conn.execute(text("""
                    INSERT INTO silver_sales_rejected (raw_row, rejection_reason, batch_id)
                    VALUES (:rr::jsonb, :reason, :bid)
                """), {
                    "rr":     json.dumps(rej["raw_row"], default=str),
                    "reason": rej["reason"],
                    "bid":    batch_id,
                })

    if df_clean.empty:
        logger.warning("⚠️ No clean rows to load into silver.")
        return

    silver_cols = [
        "row_id", "order_id", "order_date", "ship_date", "ship_mode",
        "customer_id", "customer_name", "segment",
        "country", "city", "state", "postal_code", "region",
        "product_id", "category", "sub_category", "product_name",
        "sales", "location_bk", "batch_id", "source_file",
    ]
    df_stage = df_clean[[c for c in silver_cols if c in df_clean.columns]].copy()
    df_stage.to_sql("silver_staging", db_engine, if_exists="replace", index=False)

    with db_engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO silver_sales_cleaned
                (row_id, order_id, order_date, ship_date, ship_mode,
                 customer_id, customer_name, segment,
                 country, city, state, postal_code, region,
                 product_id, category, sub_category, product_name,
                 sales, location_bk, batch_id, source_file, updated_at)
            SELECT
                row_id, order_id, order_date::date, ship_date::date, ship_mode,
                customer_id, customer_name, segment,
                country, city, state, postal_code, region,
                product_id, category, sub_category, product_name,
                sales, location_bk, batch_id, source_file, NOW()
            FROM silver_staging
            ON CONFLICT (row_id) DO UPDATE
                SET customer_name = EXCLUDED.customer_name,
                    segment       = EXCLUDED.segment,
                    product_name  = EXCLUDED.product_name,
                    sales         = EXCLUDED.sales,
                    batch_id      = EXCLUDED.batch_id,
                    source_file   = EXCLUDED.source_file,
                    updated_at    = NOW()
        """))
        upserted = result.rowcount
        log_step(conn, run_id, "silver", len(df_clean), upserted, len(rejected))

    logger.info(f"✅ Silver: {upserted} upserted | {len(rejected)} rejected")


def load_gold(df_clean: pd.DataFrame, batch_id: str, file_key: str, run_id: str):
    """
    Upsert all four dimensions, then do incremental INSERT … ON CONFLICT DO NOTHING
    for the fact table.  Surrogate keys are NEVER dropped or regenerated.
    """
    if df_clean.empty:
        return

    with db_engine.begin() as conn:

        dates = (
            df_clean[["order_date"]]
            .dropna()
            .drop_duplicates()
        )
        for _, r in dates.iterrows():
            d = r["order_date"]
            conn.execute(text("""
                INSERT INTO dim_date (date, year, month, day)
                VALUES (:d, :y, :m, :dy)
                ON CONFLICT (date) DO NOTHING
            """), {"d": d.date(), "y": int(d.year), "m": int(d.month), "dy": int(d.day)})

        customers = (
            df_clean[["customer_id", "customer_name", "segment"]]
            .drop_duplicates("customer_id")
        )
        for _, r in customers.iterrows():
            conn.execute(text("""
                INSERT INTO dim_customer (customer_id, customer_name, segment)
                VALUES (:cid, :cn, :seg)
                ON CONFLICT (customer_id) DO UPDATE
                    SET customer_name = EXCLUDED.customer_name,
                        segment       = EXCLUDED.segment,
                        updated_at    = NOW()
            """), {"cid": r["customer_id"], "cn": r["customer_name"], "seg": r["segment"]})

        products = (
            df_clean[["product_id", "product_name", "category", "sub_category"]]
            .drop_duplicates("product_id")
        )
        for _, r in products.iterrows():
            conn.execute(text("""
                INSERT INTO dim_product (product_id, product_name, category, sub_category)
                VALUES (:pid, :pn, :cat, :sub)
                ON CONFLICT (product_id) DO UPDATE
                    SET product_name = EXCLUDED.product_name,
                        category     = EXCLUDED.category,
                        sub_category = EXCLUDED.sub_category,
                        updated_at   = NOW()
            """), {"pid": r["product_id"], "pn": r["product_name"],
                   "cat": r["category"], "sub": r["sub_category"]})

        locations = (
            df_clean[["location_bk", "postal_code", "country", "city", "state", "region"]]
            .drop_duplicates("location_bk")
        )
        for _, r in locations.iterrows():
            conn.execute(text("""
                INSERT INTO dim_location (location_bk, postal_code, country, city, state, region)
                VALUES (:lbk, :pc, :co, :ci, :st, :reg)
                ON CONFLICT (location_bk) DO UPDATE
                    SET postal_code = EXCLUDED.postal_code,
                        city        = EXCLUDED.city,
                        state       = EXCLUDED.state,
                        region      = EXCLUDED.region,
                        updated_at  = NOW()
            """), {
                "lbk": r["location_bk"], "pc": r.get("postal_code"),
                "co":  r.get("country"),  "ci": r.get("city"),
                "st":  r.get("state"),   "reg": r.get("region"),
            })

        fact_inserted = conn.execute(text("""
            INSERT INTO fact_sales
                (row_id, order_id, date_id, customer_key, product_key,
                 location_key, sales, batch_id, source_file)
            SELECT
                s.row_id,
                s.order_id,
                d.date_id,
                c.customer_key,
                p.product_key,
                l.location_key,
                s.sales,
                s.batch_id,
                s.source_file
            FROM silver_sales_cleaned s
            JOIN dim_date     d ON d.date         = s.order_date
            JOIN dim_customer c ON c.customer_id  = s.customer_id
            JOIN dim_product  p ON p.product_id   = s.product_id
            JOIN dim_location l ON l.location_bk  = s.location_bk
            WHERE s.batch_id = :bid
            ON CONFLICT (row_id) DO NOTHING
        """), {"bid": batch_id}).rowcount

        log_step(conn, run_id, "gold", len(df_clean), fact_inserted, 0)

    logger.info(f"🎯 Gold: {fact_inserted} new fact rows inserted")



def run_pipeline():
    run_id    = make_run_id()
    start_time = datetime.utcnow()
    logger.info(f"🚀 Pipeline started | run_id={run_id}")

    with db_engine.begin() as conn:
        setup_all_tables(conn)
        log_run(conn, run_id, "medallion_etl", start_time, None, "RUNNING")

    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=S3_PREFIX)
        files = [
            obj["Key"]
            for obj in response.get("Contents", [])
            if obj["Key"].lower().endswith(".csv")
        ]

        if not files:
            logger.warning("⚠️ No CSV files found in S3 prefix. Nothing to process.")
            with db_engine.begin() as conn:
                log_run(conn, run_id, "medallion_etl", start_time, datetime.utcnow(), "SUCCESS")
            return

        processed = 0
        skipped   = 0

        for file_key in files:
            batch_id = str(uuid.uuid4())
            logger.info(f"📥 Checking: {file_key}")

            
            content  = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)["Body"].read()
            checksum = md5_checksum(content)

            with db_engine.begin() as conn:
                if is_file_processed(conn, file_key, checksum):
                    logger.info(f"⏭️  Skipping (already processed): {file_key}")
                    skipped += 1
                    continue

            df_raw = pd.read_csv(io.BytesIO(content))
            logger.info(f"📄 {file_key}: {len(df_raw)} raw rows")

            load_bronze(df_raw, batch_id, file_key, run_id)

            df_clean, rejected = validate_and_clean(df_raw, batch_id, file_key)
            load_silver(df_clean, rejected, batch_id, run_id)

            load_gold(df_clean, batch_id, file_key, run_id)

            with db_engine.begin() as conn:
                register_file(conn, file_key, checksum, len(df_raw), batch_id, "SUCCESS")

            processed += 1

        logger.info(f"✅ Done — {processed} files processed, {skipped} skipped")

        with db_engine.begin() as conn:
            log_run(conn, run_id, "medallion_etl", start_time, datetime.utcnow(), "SUCCESS")

    except Exception as exc:
        logger.error(f"❌ Pipeline failed: {exc}", exc_info=True)
        with db_engine.begin() as conn:
            log_run(conn, run_id, "medallion_etl", start_time, datetime.utcnow(), "FAILED", str(exc))
        raise


if __name__ == "__main__":
    run_pipeline()