from fastapi import FastAPI

app = FastAPI(title="GenAI Data Engineering API")

@app.get("/")
def read_root():
    return {"status": "✅ Backend API is running successfully!"}