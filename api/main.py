import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# --- IMPORT SERVICES ---
# [ĐÃ SỬA] Bỏ dấu chấm phía trước để thành Absolute Import
from services.user_service import init_admin_account
from routes import users, klines, orderbook, tickers, trades

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
    
    # DEBUG: In ra danh sách API đang chạy
    print("\n" + "="*40)
    print("🔍  DANH SÁCH API ĐANG HOẠT ĐỘNG:")
    for route in app.routes:
        if hasattr(route, "path"):
            methods = ", ".join(route.methods)
            print(f"📍 {methods:<10} {route.path}")
    print("="*40 + "\n")

    try:
        init_admin_account() 
        logging.info("✅ Admin account verified/created.")
    except Exception as e:
        logging.error(f"❌ Failed to init admin: {e}")
    
    yield
    logging.info("🛑 Shutting down...")

# --- KHỞI TẠO APP ---
app = FastAPI(title="BinanceAPI Hybrid", version="2.0.0", lifespan=lifespan)

# --- CẤU HÌNH CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ĐĂNG KÝ ROUTER ---
# 1. User
app.include_router(users.router) 

# 2. Orderbook
app.include_router(orderbook.router)

# 3. Market Data
app.include_router(klines.router)
app.include_router(tickers.router)
app.include_router(trades.router)

# --- ROOT ENDPOINT ---
@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "Modular Trading Backend is Running",
        "endpoints": {
            "user": "/user",
            "orders": "/orders",
            "kline": "/api/kline/{symbol}",
            "tickers": "/api/market/tickers",
            "docs": "/docs"
        }
    }

if __name__ == "__main__":
    import uvicorn
    # Chạy trực tiếp file này
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)