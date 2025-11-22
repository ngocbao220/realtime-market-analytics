"""
FastAPI Backend for Dashboard
"""

import logging

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from services.user_service import init_admin_account
from routes import trades, stats, orderbook, symbols, user

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True
)

print("Đang tạo Admin thủ công...")
init_admin_account()
print("Xong! Đã tạo Admin thủ công!!")

app = FastAPI(title="BinanceAPI", version="1.0.0")

# CORS để frontend có thể gọi API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Root endpoint
@app.get("/")
def root():
    """Health check"""
    return {
        "status": "ok",
        "message": "API is running",
        "endpoints": {
            "trades": "/api/trades", 
            "stats": "/api/stats",
            "price-history": "/api/price-history",
            "orderbook": "/api/orderbook",
            "realtime": "/api/realtime",
            "symbols": "/api/symbols"
        }
    }

# Include routers
app.include_router(stats.router, prefix="/api", tags=["Statistics"])
app.include_router(trades.router, prefix="/api", tags=["Trades"])
app.include_router(orderbook.router, prefix="/api", tags=["Orderbook"])
app.include_router(symbols.router, prefix="/api", tags=["Symbols"])
app.include_router(user.router, prefix="/user", tags=["User"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)