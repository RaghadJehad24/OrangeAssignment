import os
import re
import json
import logging
import google.generativeai as genai
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY environment variable is required")

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("models/gemini-3.1-flash-lite-preview")  


_DB_URL = (
    os.getenv("DATABASE_URL")
    or (
        f"postgresql://{os.getenv('DB_USER','admin')}:{os.getenv('DB_PASSWORD','admin123')}"
        f"@{os.getenv('DB_HOST','postgres')}:{os.getenv('DB_PORT','5432')}"
        f"/{os.getenv('DB_NAME','sales_dwh')}"
    )
)
_db_engine = create_engine(_DB_URL)

ALLOWED_TABLES = [
    "fact_sales",
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_location",
]

_schema_cache: str | None = None


def get_schema_context() -> str:
    """
    Query information_schema to build an up-to-date table/column description.
    Result is cached in-process until invalidate_schema_cache() is called.
    Falls back to a static string if the DB is unreachable.
    """
    global _schema_cache
    if _schema_cache:
        return _schema_cache

    try:
        with _db_engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT table_name, column_name, data_type
                FROM   information_schema.columns
                WHERE  table_schema = 'public'
                  AND  table_name   = ANY(:tables)
                ORDER  BY table_name, ordinal_position
            """), {"tables": ALLOWED_TABLES}).fetchall()

        if not rows:
            logger.warning("Schema introspection returned no rows — using fallback.")
            _schema_cache = _fallback_schema()
            return _schema_cache

        tables: dict[str, list[str]] = {}
        for table_name, col_name, data_type in rows:
            tables.setdefault(table_name, []).append(f"{col_name} ({data_type})")

        lines = [
            f"{i}. {tbl}({', '.join(cols)})"
            for i, (tbl, cols) in enumerate(tables.items(), 1)
        ]
        _schema_cache = "\n".join(lines)
        logger.info("✅ Schema context loaded from information_schema.")
        return _schema_cache

    except Exception as exc:
        logger.warning(f"Could not introspect schema: {exc} — using fallback.")
        _schema_cache = _fallback_schema()
        return _schema_cache


def invalidate_schema_cache() -> None:
    """Call this after any DDL change so the next request rebuilds the prompt."""
    global _schema_cache
    _schema_cache = None
    logger.info("Schema cache invalidated.")


def _fallback_schema() -> str:
    """Static fallback used when the DB is unreachable at prompt-build time."""
    return (
        "1. dim_date(date_id, date, year, month, day)\n"
        "2. dim_customer(customer_key, customer_id, customer_name, segment)\n"
        "3. dim_product(product_key, product_id, product_name, category, sub_category)\n"
        "4. dim_location(location_key, location_bk, postal_code, country, city, state, region)\n"
        "5. fact_sales(row_id, order_id, date_id, customer_key, product_key, "
        "location_key, sales, batch_id, source_file, ingested_at)"
    )



def _build_system_prompt() -> str:
    schema = get_schema_context()
    allowed = ", ".join(ALLOWED_TABLES)

    return f"""Role: You are a friendly, professional "Sales Intelligence Partner".

Languages: You are fully bilingual. Reply in the SAME language the user uses (Arabic or English).

Behavior:
1. If the user greets you or asks who you are, respond warmly in their language.
   Do NOT generate SQL for greetings.
2. For any business or data question, output ONLY a raw PostgreSQL SELECT or
   WITH query.  No markdown fences, no explanations — just the SQL code.
3. Never hallucinate or invent data.

Data Warehouse Schema (auto-generated from live database):
{schema}

Strict SQL Rules:
- Output ONLY raw SQL for data questions.
- Always use English table/column names, even when the user writes in Arabic.
- Use surrogate keys for JOINs (customer_key, product_key, location_key, date_id).
- Query ONLY these tables: {allowed}
- Do not reference any other table, system view, or schema.
"""



async def generate_sql_or_chat(user_question: str) -> str:
    """
    Send user question to Gemini with a live-schema system prompt.
    Returns either a raw SQL string or a conversational reply.
    Returns "ERROR" on failure.
    """
    try:
        system_prompt = _build_system_prompt()
        prompt = f"{system_prompt}\n\nUser Question: {user_question}\nResponse:"

        response = await model.generate_content_async(prompt)

        if not response or not response.text:
            return "عذراً، لم أستطع فهم الطلب."

        clean = re.sub(r"```sql|```", "", response.text.strip()).strip()
        return clean

    except Exception as exc:
        logger.error(f"LLM generation failed: {exc}", exc_info=True)
        return "ERROR"


async def format_data_to_natural_language(user_question: str, db_result: list) -> str:
    """
    Take raw DB rows and rephrase them as a friendly human-readable answer
    in the same language as the original question.
    """
    try:
        prompt = f"""Role: You are a friendly Sales Expert.
Task: Given the user's question and the raw query result, write a short,
      friendly, professional answer using ONLY the data provided.

Rules:
- Reply in the SAME LANGUAGE as the user's question (Arabic or English).
- Do NOT mention "database", "SQL", "query", or "JSON".
- Be concise and professional.

User Question: {user_question}
Raw Database Result: {json.dumps(db_result, ensure_ascii=False, default=str)}
Natural Answer:"""

        response = await model.generate_content_async(prompt)
        if not response or not response.text:
            return "Here are your results based on the available data."
        return response.text.strip()

    except Exception as exc:
        logger.error(f"Natural language formatting failed: {exc}", exc_info=True)
        return "إليك البيانات التي طلبتها."