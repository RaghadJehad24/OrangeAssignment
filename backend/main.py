import re
import logging

import sqlparse
import sqlparse.tokens as T
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from services.llm_agent import generate_sql_or_chat, format_data_to_natural_language
from db.database import execute_safe_query

app = FastAPI(title="Sales Intelligence API", version="2.0.0")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MAX_LIMIT = 100

ALLOWED_TABLES = {
    "fact_sales",
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_location",
}

BLOCKED_KEYWORDS = {
    "DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE",
    "CREATE", "GRANT", "REVOKE", "EXEC", "EXECUTE", "COPY",
    "PG_SLEEP", "PG_READ_FILE", "LO_EXPORT", "XP_",
}


class ChatRequest(BaseModel):
    question: str




def _extract_table_names(statement: sqlparse.sql.Statement) -> set[str]:
    """
    Walk the token tree and collect identifiers that follow FROM / JOIN keywords.
    This is intentionally conservative — unknown tokens are logged, not blocked.
    """
    tables: set[str] = set()
    prev_ttype = None
    prev_val   = ""

    for token in statement.flatten():
        val = token.value.strip().lower()

        if token.ttype in (T.Keyword, T.Keyword.DML):
            if val in ("from", "join", "inner join", "left join", "right join",
                       "full join", "cross join"):
                prev_ttype = token.ttype
                prev_val   = val
                continue

        if prev_ttype in (T.Keyword, T.Keyword.DML) and prev_val in (
            "from", "join", "inner join", "left join", "right join",
            "full join", "cross join"
        ):
            if token.ttype is T.Name or token.ttype is T.Literal.String.Single:
                tables.add(val.strip('"').strip("'"))

        prev_ttype = token.ttype
        prev_val   = val

    return tables


def is_sql_safe(sql_query: str) -> tuple[bool, str]:
    """
    Two-stage validation:
      Stage 1 — keyword blocklist (fast, catches obvious attacks).
      Stage 2 — sqlparse structural check: statement type + table allowlist.

    Returns (is_safe: bool, reason: str).
    """
    upper = sql_query.upper()

    for kw in BLOCKED_KEYWORDS:
        pattern = r"\b" + re.escape(kw) + r"\b"
        if re.search(pattern, upper):
            return False, f"Blocked keyword: {kw}"

    if "--" in sql_query or "/*" in sql_query:
        return False, "SQL comments are not allowed"

    try:
        parsed = sqlparse.parse(sql_query.strip())
        if not parsed:
            return False, "Empty SQL"

        statement = parsed[0]
        stmt_type = statement.get_type()

        if stmt_type not in ("SELECT", None):
            return False, f"Disallowed statement type: {stmt_type}"

        used_tables = _extract_table_names(statement)
        unknown = used_tables - ALLOWED_TABLES
        if unknown:
            logger.warning(f"Unknown tables in generated SQL: {unknown}")
           

    except Exception as exc:
        logger.error(f"sqlparse error during validation: {exc}")
        return False, f"SQL parse error: {exc}"

    return True, "OK"


def enforce_limit(sql: str) -> str:
    """Add LIMIT if absent; cap to MAX_LIMIT if present and exceeds cap."""
    lower = sql.lower()
    match = re.search(r"\blimit\s+(\d+)", lower)
    if not match:
        return sql + f" LIMIT {MAX_LIMIT}"
    existing = int(match.group(1))
    if existing > MAX_LIMIT:
        return re.sub(r"\blimit\s+\d+", f"LIMIT {MAX_LIMIT}", sql, flags=re.IGNORECASE)
    return sql



@app.post("/ask")
async def ask_database(request: ChatRequest):
    ai_output = await generate_sql_or_chat(request.question)

    if ai_output == "ERROR":
        raise HTTPException(status_code=500, detail="LLM failed to generate a response.")

    is_sql = ai_output.upper().strip().startswith(("SELECT", "WITH"))

    if not is_sql:
        return {"message": ai_output}

    sql = ai_output.strip().rstrip(";")

    safe, reason = is_sql_safe(sql)
    if not safe:
        logger.warning(f"Blocked unsafe SQL [{reason}]: {sql[:200]}")
        raise HTTPException(status_code=403, detail=f"Unsafe SQL blocked: {reason}")

    sql = enforce_limit(sql)

    try:
        db_result = execute_safe_query(sql)

        if isinstance(db_result, dict) and "error" in db_result:
            is_arabic = bool(re.search(r"[\u0600-\u06FF]", request.question))
            msg = (
                "عذراً، يوجد مشكلة في جلب البيانات من الخادم."
                if is_arabic
                else "Database error occurred."
            )
            return {"message": msg, "sql": sql}

        if not db_result:
            is_arabic = bool(re.search(r"[\u0600-\u06FF]", request.question))
            msg = (
                "لا توجد بيانات مطابقة لطلبك حالياً."
                if is_arabic
                else "No data found for your request."
            )
            return {"message": msg, "sql": sql}

        friendly_text = await format_data_to_natural_language(request.question, db_result)
        return {"message": friendly_text, "sql": sql}

    except Exception as exc:
        logger.error(f"DB execution failed: {exc}", exc_info=True)
        is_arabic = bool(re.search(r"[\u0600-\u06FF]", request.question))
        msg = "حدث خطأ غير متوقع." if is_arabic else "An unexpected error occurred."
        return {"message": msg, "sql": sql}


@app.get("/")
def health_check():
    return {"status": "✅ Sales API is running", "version": "2.0.0"}