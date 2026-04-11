import os
import logging
import re
import google.generativeai as genai

logger = logging.getLogger(__name__)


GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY is required")

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('models/gemini-3.1-flash-lite-preview')

SCHEMA_CONTEXT = """
You are a highly secure, read-only PostgreSQL Data Analyst. 
Your job is to translate user natural language questions (in English or Arabic) into valid, optimized PostgreSQL `SELECT` queries.

We have a Star Schema Data Warehouse with the following tables:
1. dim_date(date_id, date, year, month, day)
2. dim_customer(customer_key, customer_id, customer_name, segment)
3. dim_product(product_key, product_id, product_name, category, sub_category)
4. dim_location(location_key, postal_code, country, city, state, region)
5. fact_sales(row_id, order_id, date_id, customer_key, product_key, location_key, sales)

CRITICAL RULES:
1. Even if the user asks in Arabic, the SQL must use the English table and column names defined above.
2. Output ONLY the raw SQL query. Do not add markdown or conversational text.
3. NEVER generate DROP, DELETE, UPDATE, INSERT, ALTER, or TRUNCATE.
4. ALWAYS use Common Table Expressions (WITH clause) instead of nested subqueries for complex calculations.
5. If joining multiple tables, always use the surrogate keys (date_id, customer_key, product_key, location_key).

EXAMPLES:

User: "What is the average total revenue for each month of the year?"
SQL: 
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

User: "ما هو متوسط إجمالي قيمة الطلب؟"
SQL: 
WITH order_totals AS (
    SELECT order_id, SUM(sales) AS total_order_value
    FROM fact_sales
    GROUP BY order_id
)
SELECT AVG(total_order_value) AS average_order_value
FROM order_totals;

User: "أعطني أفضل 5 منتجات مبيعاً"
SQL:
SELECT p.product_name, SUM(f.sales) as total_sales
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_name
ORDER BY total_sales DESC
LIMIT 5;
"""


async def generate_sql_from_prompt(user_question: str) -> str:
    try:
        if "SCHEMA_CONTEXT" not in globals():
            raise ValueError("SCHEMA_CONTEXT is not defined")

        prompt = f"{SCHEMA_CONTEXT}\n\nUser Question:\n{user_question}\n\nSQL:"

        response = await model.generate_content_async(prompt)

        if not response or not response.text:
            raise ValueError("Empty response from LLM")

        raw_text = response.text.strip()
        logger.info(f"AI Raw Output:\n{raw_text}")

        clean_sql = re.sub(r"```sql|```", "", raw_text).strip()
        clean_sql = re.sub(r"http\S+", "", clean_sql).strip()

        lines = clean_sql.splitlines()
        sql_lines = [
            line for line in lines
            if not line.lower().startswith(("user:", "sql:", "explanation:"))
        ]
        clean_sql = "\n".join(sql_lines).strip()

        if clean_sql.endswith(";"):
            clean_sql = clean_sql[:-1]

        return clean_sql

    except Exception as e:
        logger.error(f"LLM Generation failed: {e}", exc_info=True)
        raise
