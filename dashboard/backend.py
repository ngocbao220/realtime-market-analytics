# File: backend.py
from fastapi import FastAPI
import redis
import json
import os

app = FastAPI()

# Cấu hình Redis (giữ nguyên như cũ)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

@app.get("/api/kline/{symbol}")
def get_kline(symbol: str, interval: str = "1m"):
    """
    Lấy dữ liệu từ Redis và map lại key cho đúng định dạng chart
    """
    # Quy tắc đặt tên key trong Redis của bạn (kiểm tra lại xem có đúng prefix kline_ chưa)
    # Nếu trong Redis bạn chỉ lưu là "BNBUSDT_1m" thì sửa dòng dưới thành: f"{symbol.upper()}_{interval}"
    redis_key = f"kline_{symbol.upper()}_{interval}" 
    
    # Lấy 200 bản ghi mới nhất
    raw_data = r.lrange(redis_key, 0, 200)
    
    mapped_data = []
    for item in raw_data:
        try:
            # 1. Parse chuỗi JSON từ Redis
            # Dữ liệu gốc: {"Symbol": "BNBUSDT", "Open": 854.68, "Open_time": "2025-11-24 14:19:00", ...}
            d = json.loads(item)
            
            # 2. Tạo dictionary mới với key viết thường để khớp với chart.py
            new_item = {
                "timestamp": d["Open_time"],  # Dùng Open_time làm mốc thời gian
                "open": float(d["Open"]),
                "high": float(d["High"]),
                "low": float(d["Low"]),
                "close": float(d["Close"]),
                "volume": float(d["Volume"])
            }
            mapped_data.append(new_item)
        except Exception as e:
            # Bỏ qua các bản ghi lỗi format
            continue
            
    # Redis List thường lưu kiểu Stack (Mới vào trước), nên dữ liệu lấy ra có thể bị ngược.
    # Nếu biểu đồ hiển thị ngược thời gian, hãy bỏ comment dòng dưới:
    # mapped_data.reverse()
    
    return {"data": mapped_data}