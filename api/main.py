"""
FastAPI Backend cho Financial Risk Dashboard
Query data từ ClickHouse và expose REST endpoints
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from clickhouse_driver import Client
from typing import Optional, List
from datetime import datetime, timedelta
from pydantic import BaseModel
import os

app = FastAPI(title="Financial Risk API", version="1.0.0")

# CORS để frontend có thể gọi API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ClickHouse connection
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "12345")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")

def get_clickhouse_client():
    """Tạo ClickHouse client"""
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE
    )

# ============================================
# MODELS
# ============================================

class TradeStats(BaseModel):
    symbol: str
    total_trades: int
    total_volume: float
    avg_price: float
    min_price: float
    max_price: float

class PricePoint(BaseModel):
    timestamp: datetime
    price: float
    volume: float

# ============================================
# ENDPOINTS
# ============================================

@app.get("/")
def root():
    """Health check"""
    return {
        "status": "ok",
        "message": "Financial Risk API is running",
        "endpoints": {
            "trades": "/api/trades",
            "stats": "/api/stats",
            "price-history": "/api/price-history",
            "orderbook": "/api/orderbook",
            "realtime": "/api/realtime"
        }
    }

@app.get("/api/stats/{symbol}")
def get_trade_stats(symbol: str, hours: int = 24):
    """
    Lấy thống kê trading của 1 symbol
    
    Args:
        symbol: Trading pair (BTCUSDT, BNBBTC, ...)
        hours: Số giờ lịch sử (default 24h)
    """
    try:
        client = get_clickhouse_client()
        
        query = f"""
        SELECT 
            Symbol as symbol,
            COUNT(*) as total_trades,
            SUM(TradeValue) as total_volume,
            AVG(Price) as avg_price,
            MIN(Price) as min_price,
            MAX(Price) as max_price
        FROM trades
        WHERE Symbol = '{symbol}'
          AND TradeTime >= now() - INTERVAL {hours} HOUR
        GROUP BY Symbol
        """
        
        result = client.execute(query)
        
        if not result:
            return {
                "symbol": symbol,
                "total_trades": 0,
                "total_volume": 0,
                "message": "No data found"
            }
        
        row = result[0]
        return {
            "symbol": row[0],
            "total_trades": row[1],
            "total_volume": round(row[2], 2),
            "avg_price": round(row[3], 2),
            "min_price": round(row[4], 2),
            "max_price": round(row[5], 2),
            "period_hours": hours
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/price-history/{symbol}")
def get_price_history(
    symbol: str, 
    interval: str = "1m",  # 1m, 5m, 15m, 1h
    limit: int = 100
):
    """
    Lấy lịch sử giá theo khoảng thời gian (OHLCV - cho candlestick)
    
    Args:
        symbol: Trading pair
        interval: Khoảng thời gian (1m, 5m, 15m, 1h)
        limit: Số điểm dữ liệu
    """
    try:
        client = get_clickhouse_client()
        
        # Map interval to ClickHouse
        interval_map = {
            "1m": "toStartOfMinute(TradeTime)",
            "5m": "toStartOfFiveMinutes(TradeTime)",
            "15m": "toStartOfFifteenMinutes(TradeTime)",
            "1h": "toStartOfHour(TradeTime)",
            "4h": "toDateTime(toStartOfHour(TradeTime) - toStartOfHour(TradeTime) % 14400)",
            "1d": "toStartOfDay(TradeTime)"
        }
        
        time_bucket = interval_map.get(interval, interval_map["1m"])
        
        # Tính OHLCV (Open, High, Low, Close, Volume) cho candlestick
        query = f"""
        SELECT 
            {time_bucket} as timestamp,
            argMin(Price, TradeTime) as open,
            MAX(Price) as high,
            MIN(Price) as low,
            argMax(Price, TradeTime) as close,
            SUM(TradeValue) as volume,
            COUNT(*) as trades
        FROM trades
        WHERE Symbol = '{symbol}'
          AND TradeTime >= now() - INTERVAL 24 HOUR
        GROUP BY timestamp
        ORDER BY timestamp DESC
        LIMIT {limit}
        """
        
        result = client.execute(query)
        
        # Tự động chọn độ chính xác dựa vào giá trị
        decimals = 8 if symbol.endswith('BTC') or symbol.endswith('ETH') else 2
        
        return {
            "symbol": symbol,
            "interval": interval,
            "data": [
                {
                    "timestamp": row[0].isoformat(),
                    "open": round(row[1], decimals),
                    "high": round(row[2], decimals),
                    "low": round(row[3], decimals),
                    "close": round(row[4], decimals),
                    "volume": round(row[5], decimals),
                    "trades": row[6]
                }
                for row in result
            ]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/realtime/{symbol}")
def get_realtime_data(symbol: str, limit: int = 10):
    """
    Lấy trades gần nhất (realtime)
    Dùng để hiển thị live trades
    """
    try:
        client = get_clickhouse_client()
        
        query = f"""
        SELECT 
            TradeTime,
            Price,
            Quantity,
            Side,
            TradeValue
        FROM trades
        WHERE Symbol = '{symbol}'
        ORDER BY TradeTime DESC
        LIMIT {limit}
        """
        
        result = client.execute(query)
        
        return {
            "symbol": symbol,
            "trades": [
                {
                    "time": row[0].isoformat(),
                    "price": row[1],
                    "quantity": row[2],
                    "side": row[3],
                    "value": row[4]
                }
                for row in result
            ]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/orderbook/{symbol}")
def get_orderbook_snapshot(symbol: str):
    """
    Lấy orderbook snapshot mới nhất
    Dùng để hiển thị bid/ask depth
    """
    try:
        client = get_clickhouse_client()
        
        query = f"""
        SELECT 
            event_time,
            bid_prices,
            bid_quantities,
            ask_prices,
            ask_quantities
        FROM orderbook
        WHERE symbol = '{symbol}'
        ORDER BY event_time DESC
        LIMIT 1
        """
        
        result = client.execute(query)
        
        if not result:
            return {"symbol": symbol, "message": "No orderbook data"}
        
        row = result[0]
        
        # Combine bid prices and quantities
        bids = [
            {"price": p, "quantity": q} 
            for p, q in zip(row[1], row[2])
        ][:10]  # Top 10 bids
        
        asks = [
            {"price": p, "quantity": q} 
            for p, q in zip(row[3], row[4])
        ][:10]  # Top 10 asks
        
        return {
            "symbol": symbol,
            "timestamp": row[0].isoformat(),
            "bids": bids,
            "asks": asks,
            "spread": asks[0]["price"] - bids[0]["price"] if bids and asks else 0
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/symbols")
def get_available_symbols():
    """Lấy danh sách symbols có data"""
    try:
        client = get_clickhouse_client()
        
        query = """
        SELECT DISTINCT Symbol 
        FROM trades
        ORDER BY Symbol
        """
        
        result = client.execute(query)
        
        return {
            "symbols": [row[0] for row in result]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
