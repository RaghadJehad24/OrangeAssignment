# 🛍️ Retail Sales GenAI Data Warehouse

## 📖 Overview

An end-to-end data engineering and AI solution that translates natural language questions into executable SQL queries against a dimensional warehouse.

| Component                  | Technology                       |
| -------------------------- | -------------------------------- |
| Object Storage (Data Lake) | LocalStack (S3-compatible)       |
| Data Warehouse             | PostgreSQL — Kimball Star Schema |
| NL-to-SQL                  | Gemini API                       |
| Backend API                | FastAPI                          |
| Chat UI                    | Streamlit                        |

---

## 🏗️ System Architecture

```text
┌────────────────────────────────────────────────────────┐
│              STREAMLIT UI  (Port 8501)                 │
│         Natural Language Chat Interface                │
└────────────────────┬───────────────────────────────────┘
                     │ HTTP POST /ask
                     ▼
┌────────────────────────────────────────────────────────┐
│              FASTAPI BACKEND  (Port 8000)              │
│  1. Receive question (English / Arabic)                │
│  2. Build prompt with live schema from information_schema│
│  3. Gemini → raw SQL                                   │
│  4. Structural SQL validation (sqlparse + allowlist)   │
│  5. Execute with 5 s timeout → JSON rows               │
│  6. Gemini → friendly natural-language answer          │
└──────┬──────────────────────────────────┬──────────────┘
       │ Gemini API                        │ SQL
       ▼                                   ▼
  ┌──────────────┐               ┌──────────────────────┐
  │  GEMINI API  │               │  PostgreSQL (DWH)    │
  │ (Translator) │               │  Star Schema         │
  └──────────────┘               └──────────┬───────────┘
                                             │
                         ┌───────────────────┼─────────────────┐
                         ▼                   ▼                 ▼
                   dim_date           dim_customer        dim_product
                   dim_location       fact_sales

┌────────────────────────────────────────────────────────┐
│          LocalStack S3  (Port 4566)                    │
│  bucket: retail-data-lake                              │
│  prefix: raw-data/   ← CSV files land here             │
└────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────┐
│          ETL RUNNER  (dedicated Docker service)        │
│  Starts only after Postgres + S3 are healthy           │
│  Runs once, exits 0 on success                        │
└────────────────────────────────────────────────────────┘
```

---

## 📊 Medallion Data Flow

```
S3 raw-data/*.csv
      │
      ▼  append-only (with batch_id, ingested_at)
 bronze_sales_raw
      │
      ▼  DQ rules → rejects → silver_sales_rejected
 silver_sales_cleaned   (upsert by row_id)
      │
      ▼  ON CONFLICT DO UPDATE — surrogate keys never regenerated
 dim_date / dim_customer / dim_product / dim_location
      │
      ▼  INSERT … ON CONFLICT DO NOTHING
 fact_sales
```

**Fact grain:** one row = one order line item (`row_id` is the business PK).

---

## 📋 Data Quality Rules

Every row in the source CSV passes through explicit, ordered checks before reaching silver:

| Rule                                                              | Action on Failure      |
| ----------------------------------------------------------------- | ---------------------- |
| Business key not null (row_id, order_id, customer_id, product_id) | Reject                 |
| order_date parseable as `DD/MM/YYYY`                              | Reject                 |
| sales is numeric                                                  | Reject                 |
| sales > 0                                                         | Reject                 |
| Duplicate row_id (same file)                                      | Keep last, deduplicate |

Rejected rows land in `silver_sales_rejected` with a `rejection_reason` column.
**Financial fields are never imputed** — no median fill.

---

## 🔑 Surrogate Key Stability

Dimension tables are created once with `CREATE TABLE IF NOT EXISTS`.
Surrogate keys (`SERIAL PRIMARY KEY`) are assigned once and never regenerated.
New or updated records are handled with `INSERT … ON CONFLICT DO UPDATE`.

```
dim_customer(customer_key ← stable forever, customer_id ← business key, …)
dim_product (product_key  ← stable forever, product_id  ← business key, …)
dim_location(location_key ← stable forever, location_bk ← MD5(postal|city|state|country), …)
```

---

## 📊 Audit & Lineage Tables

| Table                   | Purpose                                                 |
| ----------------------- | ------------------------------------------------------- |
| `etl_run_log`           | run_id, start/end time, status, error                   |
| `etl_step_log`          | source / target / rejected row counts per step          |
| `etl_file_registry`     | file checksum, batch_id — skips already-processed files |
| `silver_sales_rejected` | DQ rejects with reason                                  |

---

## 🔒 Security Architecture

| Layer             | Mechanism                                                                            |
| ----------------- | ------------------------------------------------------------------------------------ |
| LLM prompt        | System prompt names only allowed tables; schema built from live DB                   |
| Keyword blocklist | Regex blocks DROP, DELETE, UPDATE, INSERT, ALTER, TRUNCATE, COPY, pg_read_file, etc. |
| Structural parse  | `sqlparse` verifies statement type is SELECT / WITH                                  |
| Table allowlist   | Only `fact_sales`, `dim_*` may be referenced                                         |
| LIMIT cap         | Added if absent; capped at 100 if LLM exceeds it                                     |
| Statement timeout | 5 000 ms via `SET statement_timeout`                                                 |
| SQL comment block | `--` and `/*` are rejected                                                           |

---

## 🚀 Setup & Quick Start

### Prerequisites

- Docker + Docker Compose
- Gemini API key (free tier at [aistudio.google.com](https://aistudio.google.com))

### 1 — Configure environment

```bash
cp .env.example .env
# then edit .env and set GEMINI_API_KEY
```

**.env.example**

```
GEMINI_API_KEY=your_key_here
DB_USER=admin
DB_PASSWORD=admin123
DB_NAME=sales_dwh
DB_HOST=postgres
DB_PORT=5432
S3_ENDPOINT=http://localstack:4566
S3_BUCKET_NAME=retail-data-lake
```

Make sure `data/sales.csv` exists before running.

### 2 — Start

**Linux / macOS:**

```bash
chmod +x start.sh && ./start.sh
```

**Windows:**

```bat
.\start.bat
```

Both scripts:

1. Build and start all Docker services.
2. Poll until Postgres (`pg_isready`) and LocalStack (`/_localstack/health`) are genuinely ready — no fixed sleep.
3. Wait for the `etl_runner` container to finish and check its exit code.
4. Print the UI and API URLs.

### 3 — Access

| Service  | URL                        |
| -------- | -------------------------- |
| Chat UI  | http://localhost:8501      |
| API Docs | http://localhost:8000/docs |

---

## 📝 Example Queries

### Average order value

```sql
WITH order_totals AS (
    SELECT order_id, SUM(sales) AS total_order_value
    FROM fact_sales GROUP BY order_id
)
SELECT AVG(total_order_value) AS average_order_value FROM order_totals;
```

### Average monthly revenue

```sql
WITH monthly AS (
    SELECT d.year, d.month, SUM(f.sales) AS revenue
    FROM fact_sales f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY d.year, d.month
)
SELECT month, AVG(revenue) AS avg_revenue FROM monthly GROUP BY month ORDER BY month;
```

---

## 🛠️ Troubleshooting

| Symptom                       | Action                                                                                 |
| ----------------------------- | -------------------------------------------------------------------------------------- |
| ETL failed                    | `docker-compose logs etl_runner`                                                       |
| Backend cannot reach DB       | `docker-compose logs postgres`                                                         |
| LLM returns ERROR             | `docker-compose exec backend sh -c 'echo $GEMINI_API_KEY'` — also check 429 rate limit |
| Frontend cannot reach backend | `docker-compose exec frontend curl http://backend:8000/`                               |

---

## 🎯 Design Decisions & Trade-offs

| Decision                                         | Reason                                | Production alternative                              |
| ------------------------------------------------ | ------------------------------------- | --------------------------------------------------- |
| LocalStack instead of real S3                    | Local dev, no AWS account needed      | AWS S3 + IAM roles                                  |
| Pandas ETL                                       | Simple, readable, fast for demo scale | Apache Spark + Airflow                              |
| PostgreSQL as DWH                                | Available everywhere, no cloud cost   | Redshift / Snowflake / BigQuery                     |
| Gemini free tier                                 | Zero cost for assessment              | Azure OpenAI with fallback + Redis cache            |
| Simplified medallion (bronze/silver in Postgres) | Keeps stack minimal                   | True S3-parquet silver, Postgres serving layer only |

---

## 📚 Project Structure

```text
OrangeAssignment/
├── docker-compose.yml
├── .env.example        ← (Create this to show required env variables)
├── start.sh            ← Linux/Mac Start Script
├── start.bat           ← Windows Start Script
├── README.md
│
├── data/
│   └── sales.csv                # Raw dataset
│
├── backend/
│   ├── main.py                  # FastAPI application
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── db/
│   │   └── database.py          # SQLAlchemy connection & security
│   └── services/
│       └── llm_agent.py         # Gemini API Integration & Prompting
│
├── frontend/
│   ├── app.py                   # Streamlit Chatbot UI
│   ├── Dockerfile
│   └── requirements.txt
│
├── data_pipeline/
│   └── etl_script.py            # Medallion Architecture Python Script
│
└── infrastructure/
    └── init_s3.py               # Boto3 script to provision LocalStack S3
```
