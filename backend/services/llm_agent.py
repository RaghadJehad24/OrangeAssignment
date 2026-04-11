import os
import logging
import re
import json
import google.generativeai as genai

logger = logging.getLogger(__name__)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY is required")

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('models/gemini-3.1-flash-lite-preview')

SCHEMA_CONTEXT = """
Role: You are a friendly, professional "Sales Intelligence Partner".
Languages: You are fully bilingual. You MUST reply in the same language the user speaks (Arabic or English).

Behavior & Persona:
1. If the user greets you (e.g., "Hi", "مرحبا", "مين انت"), respond warmly in their language. Explain that you are a sales assistant helping track performance, customers, and revenues. Do NOT generate SQL for greetings.
2. If the user asks a business/data question (e.g., "What are our top sales?", "أعطني أفضل المنتجات مبيعاً"), you MUST output ONLY a PostgreSQL `SELECT` or `WITH` query. Do NOT add conversation, markdown, or explanations. Just the code.
3. Never hallucinate data.

Data Warehouse Schema:
1. dim_date(date_id, date, year, month, day)
2. dim_customer(customer_key, customer_id, customer_name, segment)
3. dim_product(product_key, product_id, product_name, category, sub_category)
4. dim_location(location_key, postal_code, country, city, state, region)
5. fact_sales(row_id, order_id, date_id, customer_key, product_key, location_key, sales)

Strict SQL Rules:
- Output ONLY the raw SQL code.
- Always use English table/column names in the SQL, even if the user asks in Arabic.
- Use surrogate keys for JOINs (e.g., customer_key).
"""

async def generate_sql_or_chat(user_question: str) -> str:
 
    try:
        prompt = f"{SCHEMA_CONTEXT}\n\nUser Question: {user_question}\nResponse:"
        response = await model.generate_content_async(prompt)

        if not response or not response.text:
            return "عذراً، لم أستطع فهم الطلب."

        raw_text = response.text.strip()
        clean_text = re.sub(r"```sql|```", "", raw_text).strip()
        
        return clean_text

    except Exception as e:
        logger.error(f"LLM Generation failed: {e}", exc_info=True)
        return "ERROR"

async def format_data_to_natural_language(user_question: str, db_result: list) -> str:
    
    try:
        prompt = f"""
        Role: You are a friendly Sales Expert.
        Task: Read the user's question and the raw database result, then provide a friendly, easy-to-understand conversational answer using ONLY the provided data.
        
        Rules:
        - Reply in the SAME LANGUAGE as the user's question (Arabic or English).
        - Do NOT mention "database", "SQL", "query", or "JSON".
        - Example: If the data says {{"product_name": "iPhone", "total_sales": 5000}}, say "أفضل منتج مبيعاً هو الآيفون بمبيعات إجمالية بلغت 5000" or "The top selling product is iPhone with 5,000 in sales."
        - Keep it professional and concise.

        User Question: {user_question}
        Raw Database Result: {json.dumps(db_result, ensure_ascii=False)}
        
        Natural Answer:"""
        
        response = await model.generate_content_async(prompt)
        if not response or not response.text:
            return "Here are your results based on the data."
            
        return response.text.strip()
        
    except Exception as e:
        logger.error(f"Formatting failed: {e}", exc_info=True)
        return "إليك البيانات التي طلبتها." 