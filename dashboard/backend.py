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

# --- API LẤY ORDERBOOK (SỔ LỆNH) ---
@app.get("/api/orderbook/{symbol}")
def get_orderbook(symbol: str):
    redis_key = f"orderbook:{symbol.upper()}"
    raw_data = r.get(redis_key)
    
    if not raw_data:
        return {"bids": [], "asks": []}
        
    try:
        d = json.loads(raw_data)
        # Map lại cấu trúc cho frontend
        # Redis: "Bid_prices": [...], "Bid_quantities": [...]
        # Frontend cần: [[Price, Amount], [Price, Amount]]
        
        bids = []
        if "Bid_prices" in d and "Bid_quantities" in d:
            for p, q in zip(d["Bid_prices"], d["Bid_quantities"]):
                 bids.append([str(p), str(q)]) # Chuyển về string để hiển thị đẹp
                 
        asks = []
        if "Ask_prices" in d and "Ask_quantities" in d:
             for p, q in zip(d["Ask_prices"], d["Ask_quantities"]):
                 asks.append([str(p), str(q)])
                 
        return {"bids": bids[:10], "asks": asks[:10]} # Lấy top 10
    except:
        return {"bids": [], "asks": []}
# --- API LẤY KLINE (BIỂU ĐỒ) ---
@app.get("/api/kline/{symbol}")
def get_kline(symbol: str, interval: str = "1m"):
    # 1. Sửa format key: Dùng dấu : thay vì _
    redis_key = f"kline:{symbol.upper()}:{interval}"
    
    # 2. Lấy dữ liệu dạng String
    raw_data = r.get(redis_key)
    
    if not raw_data:
        # Fallback: Nếu không tìm thấy, thử tìm key cũ xem sao
        redis_key_alt = f"kline_{symbol.upper()}_{interval}"
        raw_data = r.get(redis_key_alt)
        
    if not raw_data:
        return {"data": []} # Không có dữ liệu

    mapped_data = []
    try:
        # Dữ liệu của bạn là 1 JSON object duy nhất, không phải List
        # Ví dụ: {"Symbol": "BTCUSDT", "Open": 86931.26, ...}
        d = json.loads(raw_data)
        
        # Vì Spark đang ghi đè giá trị mới nhất, ta chỉ có 1 cây nến duy nhất tại thời điểm này.
        # Để vẽ biểu đồ đẹp, ta cần lưu lịch sử. Nhưng tạm thời hiển thị cái mới nhất đã.
        new_item = {
            "timestamp": d.get("Open_time"), 
            "open": float(d.get("Open", 0)),
            "high": float(d.get("High", 0)),
            "low": float(d.get("Low", 0)),
            "close": float(d.get("Close", 0)),
            "volume": float(d.get("Volume", 0))
        }
        mapped_data.append(new_item)
    except Exception as e:
        print(f"Lỗi parse JSON: {e}")
            
    return {"data": mapped_data}