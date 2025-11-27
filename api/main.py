import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from services.user_service import init_special_account
from routers import users, orders, market, klines, tickers, narrative
from db import init_db

# --- CẤU HÌNH LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True
)

# --- LIFESPAN (Hợp nhất logic khởi tạo) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("🚀 Starting Trading Intelligence Backend...")

    try:
        # 1. Khởi tạo tài khoản Admin/Special (Logic cũ)
        init_special_account() 
        logging.info("✅ Admin account verified/created.")
        
        # 2. Khởi tạo Database cho Tin tức/Narrative (Logic mới)
        init_db()
        logging.info("✅ News/Narrative DB initialized.")
        
    except Exception as e:
        logging.error(f"❌ Failed to initialize system: {e}")
    
    yield
    logging.info("🛑 Shutting down...")

# --- KHỞI TẠO APP ---
app = FastAPI(title="Crypto Trading Intelligence API", version="2.1.0", lifespan=lifespan)

# --- CẤU HÌNH CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ĐĂNG KÝ ROUTER (Backend Trading) ---
app.include_router(users.router)
app.include_router(orders.router)
app.include_router(market.router)
app.include_router(klines.router)
app.include_router(tickers.router)
app.include_router(narrative.router)

# --- ROOT ENDPOINT (Hợp nhất response) ---
@app.get("/")
def root():
    return {
        "status": "ok",
        "system": "Trading Backend + GraphRAG",
        "ai_module": "Gemini 2.5 Flash Ready",
        "message": "System is running fully operational",
    }

if __name__ == "__main__":
    import uvicorn
    # Chạy trực tiếp file này
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)