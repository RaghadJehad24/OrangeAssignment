import re
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from services.llm_agent import generate_sql_from_prompt
from db.database import execute_safe_query

app = FastAPI(title="GenAI Data Engineering API", version="1.1.0")

logger = logging.getLogger(__name__)

class ChatRequest(BaseModel):
    question: str

def is_sql_safe(sql_query: str) -> bool:
    dangerous_keywords = [
        r"\bDROP\b",
        r"\bDELETE\b",
        r"\bUPDATE\b",
        r"\bINSERT\b",
        r"\bALTER\b",
        r"\bTRUNCATE\b",
        r"\bCREATE\b",    
        r"\bGRANT\b",     
        r"\bREVOKE\b",    
        r"\bEXEC\b",      
        r"\bCAST\b",  
    ]
    upper_query = sql_query.upper()
    for keyword in dangerous_keywords:
        if re.search(keyword, upper_query):
            return False
    return True

def validate_sql_structure(sql_query: str) -> bool:
    sql = sql_query.strip().lower()
    if not (sql.startswith("select") or sql.startswith("with")):
        return False
    return True

@app.post("/ask")
async def ask_database(request: ChatRequest):


    generated_sql = generate_sql_from_prompt(request.question)
    if generated_sql == "ERROR":
        raise HTTPException(status_code=500, detail="LLM failed to generate query.")

    if not is_sql_safe(generated_sql):
        raise HTTPException(status_code=403, detail="Unsafe SQL detected.")

    if not validate_sql_structure(generated_sql):
        raise HTTPException(status_code=400, detail="Invalid SQL structure.")

    if "limit" not in generated_sql.lower():
        generated_sql += " LIMIT 100"

    result = execute_safe_query(generated_sql)

    return {
        "question": request.question,
        "sql": generated_sql,
        "result": result
    }


@app.get("/")
def health_check():
    return {"status": "✅ API is running"}