import re
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from services.llm_agent import generate_sql_or_chat, format_data_to_natural_language
from db.database import execute_safe_query

app = FastAPI(title="Sales Intelligence API", version="1.2.0")

logger = logging.getLogger(__name__)

class ChatRequest(BaseModel):
    question: str

def is_sql_safe(sql_query: str) -> bool:
    dangerous_keywords = [
        r"\bDROP\b", r"\bDELETE\b", r"\bUPDATE\b", r"\bINSERT\b", 
        r"\bALTER\b", r"\bTRUNCATE\b", r"\bCREATE\b", r"\bGRANT\b", 
        r"\bREVOKE\b", r"\bEXEC\b"
    ]
    upper_query = sql_query.upper()
    for keyword in dangerous_keywords:
        if re.search(keyword, upper_query):
            return False
    return True

@app.post("/ask")
async def ask_database(request: ChatRequest):
    ai_output = await generate_sql_or_chat(request.question)
    
    if ai_output == "ERROR":
        raise HTTPException(status_code=500, detail="LLM failed to generate a response.")

    is_sql = ai_output.upper().strip().startswith(("SELECT", "WITH"))

    if is_sql:
        if not is_sql_safe(ai_output):
            raise HTTPException(status_code=403, detail="Unsafe SQL detected.")
            ai_output = ai_output.strip()
        if ai_output.endswith(";"):
            ai_output = ai_output[:-1]
            
        if "limit" not in ai_output.lower():
            ai_output += " LIMIT 50"
    

    
        try:
            db_result = execute_safe_query(ai_output)
            
            if isinstance(db_result, dict) and "error" in db_result:
                is_arabic = bool(re.search(r'[\u0600-\u06FF]', request.question))
                err_msg = "عذراً، يوجد مشكلة في جلب البيانات من الخادم (Database Error)." if is_arabic else "Database error occurred."
                return {"message": err_msg, "sql": ai_output}

            if not db_result:
                is_arabic = bool(re.search(r'[\u0600-\u06FF]', request.question))
                msg = "لا توجد مبيعات أو بيانات مسجلة مطابقة لطلبك حالياً." if is_arabic else "No sales data found for your request."
                return {"message": msg, "sql": ai_output}

            friendly_text = await format_data_to_natural_language(request.question, db_result)

            return {
                "message": friendly_text,
                "sql": ai_output 
            }
            
        except Exception as e:
            logger.error(f"DB Execution failed: {e}", exc_info=True)
            return {"message": "حدث خطأ غير متوقع.", "sql": ai_output}
            
    else:
     
        return {
            "message": ai_output
        }

@app.get("/")
def health_check():
    return {"status": "✅ Sales API is running"}