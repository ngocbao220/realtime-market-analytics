from fastapi import FastAPI
import redis
import json
import os

app = FastAPI()

# Cấu hình Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Kết nối Redis
try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
except Exception as e:
    print(f"Redis connection error: {e}")

# --- ĐÂY LÀ PHẦN BẠN ĐANG THIẾU (để sửa lỗi 404) ---
@app.get("/")
def read_root():
    return {"message": "Backend API đang chạy ổn định!"}
# ---------------------------------------------------

@app.get("/api/kline/{symbol}")
def get_kline(symbol: str, interval: str = "1m"):
    """
    Lấy dữ liệu từ Redis, parse JSON và map key viết hoa -> viết thường
    """
    # Giả định key Redis là: kline_BNBUSDT_1m
    # Bạn cần kiểm tra lại chính xác tên key trong Redis của bạn
    redis_key = f"kline_{symbol.upper()}_{interval}"
    
    # Lấy 200 nến mới nhất
    raw_data = r.lrange(redis_key, 0, 200)
    
    mapped_data = []
    for item in raw_data:
        try:
            # Parse JSON gốc từ Redis (có key viết Hoa: Open, High...)
            d = json.loads(item)
            
            # Map sang format mà Chart.py cần (key viết thường: open, high...)
            new_item = {
                "timestamp": d.get("Open_time"), # Lấy chuỗi thời gian
                "open": float(d.get("Open", 0)),
                "high": float(d.get("High", 0)),
                "low": float(d.get("Low", 0)),
                "close": float(d.get("Close", 0)),
                "volume": float(d.get("Volume", 0))
            }
            mapped_data.append(new_item)
        except Exception as e:
            continue
            
    # Nếu dữ liệu trong Redis lưu theo kiểu Stack (Mới trước -> Cũ sau), cần đảo ngược lại cho biểu đồ
    # mapped_data.reverse() 

    return {"data": mapped_data}