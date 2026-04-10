````markdown
# 🛍️ Retail Sales GenAI Data Warehouse - Technical Assessment

## 📖 Overview

A complete, end-to-end data engineering and AI solution that translates natural language questions into executable SQL queries. This system utilizes:

- ☁️ **LocalStack** to emulate AWS S3 (Data Lake)
- 🗄️ **PostgreSQL** as a Data Warehouse utilizing a Kimball Star Schema
- 🤖 **Gemini API** for Natural Language to SQL translation
- 🔌 **FastAPI** for a robust, secure Backend API
- 🎨 **Streamlit** for an interactive Frontend Chatbot UI

---

## 🏗️ System Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│                     STREAMLIT UI (Port 8501)                │
│              Chat Interface for Natural Language            │
└────────────────────────┬────────────────────────────────────┘
                         │ HTTP POST Request
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   FASTAPI BACKEND (Port 8000)               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ 1. Receive User Query (English/Arabic)               │   │
│  │ 2. Send context to LLM → Generate SQL                │   │
│  │ 3. Execute Security Checks (Regex & Structure)       │   │
│  │ 4. Execute safe SQL against Data Warehouse           │   │
│  │ 5. Return JSON results & generated SQL               │   │
│  └──────────────────────────────────────────────────────┘   │
└────────┬─────────────────────────────┬──────────────────────┘
         │                             │
         │ LLM API Call                │ SQL Execution
         ▼                             ▼
    ┌─────────────────┐       ┌──────────────────────┐
    │  GEMINI API     │       │  PostgreSQL (DWH)    │
    │  (Translator)   │       │  (Star Schema)       │
    └─────────────────┘       └────────┬─────────────┘
                                       │
                              ┌────────┴────────┐
                              ▼                 ▼
                        ┌──────────────┐  ┌──────────────┐
                        │  dim_date    │  │ dim_customer │
                        │  dim_product │  │ dim_location │
                        │  fact_sales  │  │              │
                        └──────────────┘  └──────────────┘

┌─────────────────────────────────────────────────────────────┐
│              LocalStack (S3 Emulator - Port 4566)           │
│  ├── /raw-data/sales.csv  ← Raw Data File                   │
│  └── retail-data-lake bucket                                │
└─────────────────────────────────────────────────────────────┘
```
````

---

## 📊 Data Flow Lifecycle

**From user input to UI display:**

1. User types a question (e.g., _"What is the average order value?"_).
2. **Streamlit** sends a `POST /ask` request with the payload.
3. **FastAPI Backend** intercepts the request.
4. **LLM Agent (Gemini)**:
   - Reads the `SCHEMA_CONTEXT` (Table names, columns, and rules).
   - Translates the intent into an optimized PostgreSQL query.
5. **Backend Security Filter (3 Layers)**:
   - ✅ Blocks destructive keywords (`DROP`, `DELETE`, `UPDATE`, etc.).
   - ✅ Ensures the query starts with `SELECT` or `WITH`.
   - ✅ Enforces a `LIMIT` clause to prevent data dumps.
6. The safe SQL query is executed on **Postgres** with a 5-second statement timeout.
7. Results are mapped to a JSON array.
8. **Streamlit** renders the final answer alongside the generated SQL code block.

---

## 🚀 Setup & Installation (Step-by-Step)

### Prerequisites:

- Docker & Docker Compose
- Gemini API Key (Free from Google AI Studio)
- Terminal / Bash

### 1. Clone & Configure Environment

```bash
# 1. Clone the repository
git clone <repo_url>
cd OrangeAssignment

# 2. Create the .env file
cat > .env << EOF
# Gemini API
GEMINI_API_KEY=your_actual_api_key_here

# Database
DB_USER=admin
DB_PASSWORD=admin123
DB_NAME=sales_dwh
DB_HOST=postgres
DB_PORT=5432

# S3 / LocalStack
S3_ENDPOINT=http://localstack:4566
S3_BUCKET_NAME=retail-data-lake

# Backend
API_URL=http://backend:8000
EOF

# 3. Ensure sales.csv is in the root directory
ls -la sales.csv
```

### 2. Build & Spin Up Containers

```bash
# Build images and start detached containers
docker-compose up -d --build

# Verify status (all should be Up/Healthy)
docker-compose ps
```

### 3. Data Initialization (S3 Provisioning & ETL)

```bash
# Wait ~10 seconds for DB and LocalStack to be fully ready
sleep 10

# 1. Provision S3 Bucket and upload raw data
docker-compose exec backend python infrastructure/init_s3.py

# 2. Run the Medallion ETL Pipeline (Bronze -> Silver -> Gold)
docker-compose exec backend python data_pipeline/etl_script.py

# 3. Verify DWH ingestion
docker-compose exec postgres psql -U admin -d sales_dwh -c "SELECT COUNT(*) FROM fact_sales;"
```

### 4. Access the Application

- **Frontend UI (Streamlit):** [http://localhost:8501](http://localhost:8501)
- **API Docs (Swagger):** [http://localhost:8000/docs](http://localhost:8000/docs)

---

## 📝 Query Examples (Part II Deliverables)

### 1. Average Order Value

**User Prompt:** `What is the average order value?`

```sql
WITH order_totals AS (
    SELECT order_id, SUM(sales) AS total_order_value
    FROM fact_sales
    GROUP BY order_id
)
SELECT AVG(total_order_value) AS average_order_value
FROM order_totals;
```

### 2. Average Monthly Revenue

**User Prompt:** `What is the average total revenue for each month?`

```sql
WITH monthly_sales AS (
    SELECT d.year, d.month, SUM(f.sales) AS total_revenue
    FROM fact_sales f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY d.year, d.month
)
SELECT month, AVG(total_revenue) AS average_revenue
FROM monthly_sales
GROUP BY month
ORDER BY month;
```

---

## 🔒 Security Measures

1. **SQL Injection Prevention:** Used `SQLAlchemy` parameterized execution options where applicable.
2. **Statement Timeout:** `connection.execute(text("SET statement_timeout = 5000;"))` limits execution to 5 seconds.
3. **Keyword Blacklist:** Regex blocks destructive operations (`DROP`, `ALTER`, `TRUNCATE`, etc.).
4. **Structure Enforcement:** Only permits queries starting with `SELECT` or `WITH`.
5. **LIMIT Enforcement:** Automatically appends `LIMIT 100` if omitted by the LLM.

---

## 📊 Star Schema Design

The Data Warehouse implements Kimball's Star Schema methodology for OLAP optimization.

**Dimensions:**

- `dim_date` (date_id, date, year, month, day)
- `dim_customer` (customer_key, customer_id, customer_name, segment)
- `dim_product` (product_key, product_id, product_name, category, sub_category)
- `dim_location` (location_key, postal_code, country, city, state, region)

**Fact Table (`fact_sales`):**
Utilizes **Surrogate Keys** linking to dimensions, optimized for heavy aggregation queries.

---

## 🛠️ Troubleshooting

- **Backend cannot connect to Database:**
  Run `docker-compose logs postgres` or manually test via `docker-compose exec postgres psql -U admin -d sales_dwh`.
- **Frontend cannot reach Backend:**
  Ensure they share the same Docker network. Test with `docker-compose exec frontend curl http://backend:8000/`.
- **LLM Fails to generate SQL:**
  Verify your API key via `docker-compose exec backend echo $GEMINI_API_KEY`. A `429 Error` means you hit the free-tier rate limit; wait 60 seconds and retry.

---

## 🎯 Architectural Trade-offs & Production Readiness

**What I did for Fast Prototyping:**

- Emulated S3 locally using `LocalStack`.
- Processed ETL in-memory using `Pandas`.
- Built the UI with `Streamlit` for rapid deployment.

**How I would design this for a true Production Environment:**

- 🚀 **Infrastructure:** Use real managed services (AWS S3, Amazon Redshift / Snowflake for DWH).
- 🚀 **ETL Engine:** Replace Pandas with distributed computing (Apache Spark) orchestrated by Airflow.
- 🚀 **LLM Reliability:** Implement an LLM Fallback (e.g., OpenAI API) and Redis Caching for frequent queries to avoid rate limits and reduce latency.
- 🚀 **Security:** Transition from a Keyword Blacklist to a strict Column/Table Whitelist (Semantic Layer).

---

## 📚 Project Structure

```text
OrangeAssignment/
├── docker-compose.yml
├── .env
├── sales.csv
├── README.md
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
