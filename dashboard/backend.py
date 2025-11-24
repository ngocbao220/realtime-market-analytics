from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
import json
import os
import time

app = FastAPI()

# --- CẤU HÌNH REDIS ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

try:
    # decode_responses=True để nhận về String thay vì Bytes
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
except Exception as e:
    print(f"❌ Lỗi kết nối Redis: {e}")

# --- MODELS (Dùng để hứng dữ liệu từ request) ---
class UserRequest(BaseModel):
    username: str

class OrderRequest(BaseModel):
    user_id: str
    price: float
    amount: float

# --- ROOT ENDPOINT ---
@app.get("/")
def read_root():
    return {"message": "Backend Trading System đang chạy!"}

# ============================================================
# 1. API USER (QUẢN LÝ NGƯỜI DÙNG)
# ============================================================

@app.post("/user/create")
def create_user(user: UserRequest):
    """
    Tạo user mới hoặc trả về user cũ nếu đã tồn tại.
    Lưu trong Redis Hash: users_data
    """
    username = user.username.strip()
    
    # 1. Kiểm tra xem username đã có trong Redis chưa
    # Ta quét qua các user để tìm tên (đơn giản hóa cho demo)
    all_keys = r.keys("user:*")
    for key in all_keys:
        data = r.hgetall(key)
        if data.get("username") == username:
            return data # Trả về user cũ nếu đã tồn tại

    # 2. Nếu chưa có, tạo mới
    # Tăng ID tự động
    new_id = r.incr("user_id_counter") 
    user_key = f"user:{new_id}"
    
    new_user_data = {
        "user_id": str(new_id),
        "username": username,
        "usd": 50000.0,   # Tặng sẵn 50k USD
        "btc": 0.0,       # 0 BTC
    }
    
    # Lưu vào Redis (Hash map)
    r.hset(user_key, mapping=new_user_data)
    
    return new_user_data

@app.get("/user/get/{user_id}")
def get_user(user_id: str):
    """Lấy thông tin số dư user"""
    # Nếu là user 0 (Admin giả lập)
    if user_id == "0":
        return {"user_id": "0", "username": "ADMIN", "usd": 999999999, "btc": 999}

    user_key = f"user:{user_id}"
    if not r.exists(user_key):
        raise HTTPException(status_code=404, detail="User not found")
    
    data = r.hgetall(user_key)
    # Convert số về dạng float để frontend dễ tính toán
    data["usd"] = float(data.get("usd", 0))
    data["btc"] = float(data.get("btc", 0))
    return data

# ============================================================
# 2. API ORDER (ĐẶT LỆNH MUA/BÁN)
# ============================================================

@app.post("/orders/{side}")
def place_order(side: str, order: OrderRequest):
    """
    Xử lý lệnh Buy/Sell và cập nhật số dư
    """
    user_key = f"user:{order.user_id}"
    
    if not r.exists(user_key):
        raise HTTPException(status_code=404, detail="User không tồn tại")
    
    # Lấy thông tin user hiện tại
    user_data = r.hgetall(user_key)
    current_usd = float(user_data.get("usd", 0))
    current_btc = float(user_data.get("btc", 0))
    
    total_cost = order.price * order.amount

    if side == "buy":
        if current_usd >= total_cost:
            new_usd = current_usd - total_cost
            new_btc = current_btc + order.amount
            
            # Cập nhật Redis
            r.hset(user_key, mapping={"usd": new_usd, "btc": new_btc})
            return {"status": "success", "message": f"Đã mua {order.amount} BTC", "new_balance": {"usd": new_usd, "btc": new_btc}}
        else:
            return {"status": "failed", "detail": "Số dư USD không đủ"}

    elif side == "sell":
        if current_btc >= order.amount:
            new_usd = current_usd + total_cost
            new_btc = current_btc - order.amount
            
            # Cập nhật Redis
            r.hset(user_key, mapping={"usd": new_usd, "btc": new_btc})
            return {"status": "success", "message": f"Đã bán {order.amount} BTC", "new_balance": {"usd": new_usd, "btc": new_btc}}
        else:
            return {"status": "failed", "detail": "Số dư BTC không đủ"}
    
    else:
        raise HTTPException(status_code=400, detail="Side phải là 'buy' hoặc 'sell'")

# ============================================================
# 3. API KLINE (DỮ LIỆU BIỂU ĐỒ - GIỮ NGUYÊN CŨ)
# ============================================================

@app.get("/api/kline/{symbol}")
def get_kline(symbol: str, interval: str = "1m"):
    redis_key = f"kline_{symbol.upper()}_{interval}"
    raw_data = r.lrange(redis_key, 0, 200)
    
    mapped_data = []
    for item in raw_data:
        try:
            d = json.loads(item)
            new_item = {
                "timestamp": d.get("Open_time"), 
                "open": float(d.get("Open", 0)),
                "high": float(d.get("High", 0)),
                "low": float(d.get("Low", 0)),
                "close": float(d.get("Close", 0)),
                "volume": float(d.get("Volume", 0))
            }
            mapped_data.append(new_item)
        except:
            continue
            
    return {"data": mapped_data}