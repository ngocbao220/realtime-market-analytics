import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from services.user_service import init_special_account
from routers import users, orders, market, klines

# --- CẤU HÌNH LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True
)

# --- LIFESPAN ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("🚀 Starting Trading Backend...")

    try:
        init_special_account() 
        logging.info("✅ Admin account verified/created.")
    except Exception as e:
        logging.error(f"❌ Failed to init admin: {e}")
    
    yield
    logging.info("🛑 Shutting down...")

# --- KHỞI TẠO APP ---
app = FastAPI(title="Crypto API", version="2.0.0", lifespan=lifespan)

# --- CẤU HÌNH CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ĐĂNG KÝ ROUTER ---
app.include_router(users.router)
app.include_router(orders.router)
app.include_router(market.router)
app.include_router(klines.router)

# --- ROOT ENDPOINT ---
@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "Trading Backend is Running",
    }

if __name__ == "__main__":
    import uvicorn
    # Chạy trực tiếp file này
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)