#!/usr/bin/env python3
"""Stock DB webhook — receives updated DB from GitHub Actions."""
import os, json, shutil, sqlite3
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse
import uvicorn

app = FastAPI(title="Stock DB Webhook")
DB_PATH = os.path.expanduser("~/.hermes/databases/stocks.db")
API_KEY = os.environ.get("STOCK_API_KEY", "")

if not API_KEY:
    API_KEY = os.urandom(16).hex()
    print(f"⚠️  No STOCK_API_KEY set, generated: {API_KEY}")
    print(f"   Set STOCK_API_KEY env var for stability")

@app.get("/health")
def health():
    return {"status": "ok", "db": os.path.exists(DB_PATH)}

@app.get("/db")
def download_db():
    """Serve current DB for GitHub Actions to download."""
    if not os.path.exists(DB_PATH):
        raise HTTPException(404, "DB not found")
    return FileResponse(DB_PATH, media_type="application/octet-stream", filename="stocks.db")

@app.post("/ingest")
async def ingest(db: UploadFile = File(...), result: str = Form(None), authorization: str = ""):
    if authorization != f"Bearer {API_KEY}":
        raise HTTPException(403, "Invalid API key")
    
    # Backup
    if os.path.exists(DB_PATH):
        shutil.copy2(DB_PATH, DB_PATH + ".bak")
    
    # Save new DB
    content = await db.read()
    with open(DB_PATH + ".tmp", "wb") as f:
        f.write(content)
    os.replace(DB_PATH + ".tmp", DB_PATH)
    
    # Verify
    conn = sqlite3.connect(DB_PATH)
    latest = conn.execute("SELECT MAX(date) FROM stock_ohlcv").fetchone()[0]
    stocks = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_ohlcv WHERE date=?", (latest,)).fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM stock_ohlcv").fetchone()[0]
    conn.close()
    
    size = round(os.path.getsize(DB_PATH) / 1024 / 1024, 1)
    result_data = json.loads(result) if result else {}
    
    print(f"✅ DB updated! Latest: {latest}, Stocks: {stocks}, Total: {total}, Size: {size}MB")
    
    return {
        "status": "ok", "latest_date": latest,
        "stocks_updated": stocks, "total_rows": total, "size_mb": size,
        "from": result_data.get("timestamp", "")
    }

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8765))
    print(f"🚀 Stock DB Webhook :{port}")
    print(f"🔑 API Key: {API_KEY[:16]}...")
    print(f"📁 DB: {DB_PATH} ({os.path.getsize(DB_PATH)/1024/1024:.0f}MB)" if os.path.exists(DB_PATH) else "📁 DB: not found")
    uvicorn.run(app, host="0.0.0.0", port=port)
