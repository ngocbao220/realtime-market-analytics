import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# --- CẤU HÌNH LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- IMPORT ROUTERS ---
# Dùng try-except để tránh lỗi sập server nếu thiếu file
try:
    from routers import users, orders, market, klines, tickers
    has_routers = True
except ImportError as e:
    logger.error(f"❌ Lỗi Import Routers: {e}")
    has_routers = False

# Mock service
try:
    from services.user_service import init_special_account
except ImportError:
    def init_special_account():
        logger.warning("⚠️ Mock init_special_account")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Starting Trading Backend...")
    try:
        init_special_account()
    except Exception:
        pass
    yield
    logger.info("🛑 Shutting down...")

app = FastAPI(title="Crypto API", version="2.0.0", lifespan=lifespan)

# --- KHẮC PHỤC LỖI CORS ---
# Quan trọng: Dùng ["*"] để cho phép cả localhost, 127.0.0.1 và redirect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ĐĂNG KÝ ROUTER ---
if has_routers:
    app.include_router(users.router)
    app.include_router(orders.router)
    app.include_router(market.router)
    app.include_router(klines.router)
    app.include_router(tickers.router)

@app.get("/")
def root():
    return {"status": "ok", "message": "Backend Running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)