from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from clickhouse_driver import Client
import redis
import json
import os
from datetime import datetime

app = FastAPI()

# ============================================================
# 1. CẤU HÌNH KẾT NỐI (REDIS & CLICKHOUSE)
# ============================================================

# --- REDIS (Hot Data) ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
except Exception as e:
    print(f"❌ Lỗi kết nối Redis: {e}")

# --- CLICKHOUSE (Cold Data) ---
# Đảm bảo thông tin này khớp với server ClickHouse của bạn
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = 9000 
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "12345")
CLICKHOUSE_DB = "default"

try:
    ch_client = Client(
        host=CLICKHOUSE_HOST, 
        port=CLICKHOUSE_PORT, 
        user=CLICKHOUSE_USER, 
        password=CLICKHOUSE_PASSWORD, 
        database=CLICKHOUSE_DB
    )
except Exception as e:
    print(f"❌ Lỗi cấu hình ClickHouse client: {e}")

# --- MODELS ---
class UserRequest(BaseModel):
    username: str

class OrderRequest(BaseModel):
    user_id: str
    price: float
    amount: float

@app.get("/")
def read_root():
    return {"message": "Hybrid Trading Backend đang chạy!"}

# ============================================================
# 2. API USER (QUẢN LÝ NGƯỜI DÙNG) - Giữ nguyên code của bạn
# ============================================================

@app.post("/user/create")
def create_user(user: UserRequest):
    username = user.username.strip()
    all_keys = r.keys("user:*")
    for key in all_keys:
        data = r.hgetall(key)
        if data.get("username") == username:
            return data 
    new_id = r.incr("user_id_counter") 
    user_key = f"user:{new_id}"
    new_user_data = {"user_id": str(new_id), "username": username, "usd": 50000.0, "btc": 0.0}
    r.hset(user_key, mapping=new_user_data)
    return new_user_data

@app.get("/user/get/{user_id}")
def get_user(user_id: str):
    if user_id == "0":
        return {"user_id": "0", "username": "ADMIN", "usd": 999999999, "btc": 999}
    user_key = f"user:{user_id}"
    if not r.exists(user_key):
        raise HTTPException(status_code=404, detail="User not found")
    data = r.hgetall(user_key)
    data["usd"] = float(data.get("usd", 0))
    data["btc"] = float(data.get("btc", 0))
    return data

# ============================================================
# 3. API ORDER (ĐẶT LỆNH) - Bổ sung lại (vì code bạn gửi thiếu)
# ============================================================

@app.post("/orders/{side}")
def place_order(side: str, order: OrderRequest):
    user_key = f"user:{order.user_id}"
    if not r.exists(user_key):
        raise HTTPException(status_code=404, detail="User không tồn tại")
    
    user_data = r.hgetall(user_key)
    current_usd = float(user_data.get("usd", 0))
    current_btc = float(user_data.get("btc", 0))
    total_cost = order.price * order.amount

    if side == "buy":
        if current_usd >= total_cost:
            new_usd = current_usd - total_cost
            new_btc = current_btc + order.amount
            r.hset(user_key, mapping={"usd": new_usd, "btc": new_btc})
            return {"status": "success"}
        else:
            return {"status": "failed", "detail": "Số dư USD không đủ"}
    elif side == "sell":
        if current_btc >= order.amount:
            new_usd = current_usd + total_cost
            new_btc = current_btc - order.amount
            r.hset(user_key, mapping={"usd": new_usd, "btc": new_btc})
            return {"status": "success"}
        else:
            return {"status": "failed", "detail": "Số dư BTC không đủ"}
    return {"status": "failed", "detail": "Lỗi tham số"}

# ============================================================
# 4. API ORDERBOOK - Giữ nguyên code của bạn
# ============================================================

@app.get("/api/orderbook/{symbol}")
def get_orderbook(symbol: str):
    redis_key = f"orderbook:{symbol.upper()}"
    raw_data = r.get(redis_key)
    if not raw_data:
        return {"bids": [], "asks": []} 
    try:
        d = json.loads(raw_data)
        bids = []
        if "Bid_prices" in d and "Bid_quantities" in d:
            for p, q in zip(d["Bid_prices"], d["Bid_quantities"]):
                 bids.append([str(p), str(q)])
        asks = []
        if "Ask_prices" in d and "Ask_quantities" in d:
             for p, q in zip(d["Ask_prices"], d["Ask_quantities"]):
                 asks.append([str(p), str(q)])
        return {"bids": bids[:10], "asks": asks[:10]}
    except:
        return {"bids": [], "asks": []}

# ============================================================
# 5. API KLINE (HYBRID: CLICKHOUSE + REDIS)
# ============================================================

@app.get("/api/kline/{symbol}")
def get_kline_hybrid(symbol: str, interval: str = "1m"):
    symbol = symbol.upper()
    
    # --- A. LẤY LỊCH SỬ TỪ CLICKHOUSE ---
    history_data = []
    try:
        query = f"""
            SELECT Open_time, Open, High, Low, Close, Volume 
            FROM klines 
            WHERE Symbol = '{symbol}' AND Interval = '{interval}'
            ORDER BY Open_time DESC 
            LIMIT 99
        """
        rows = ch_client.execute(query)
        for row in rows:
            ts = row[0]
            if isinstance(ts, datetime):
                ts = str(ts)
            history_data.append({
                "timestamp": ts,
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5])
            })
        history_data.reverse() # Sắp xếp cũ -> mới
    except Exception as e:
        print(f"⚠️ ClickHouse Warning: {e}")

    # --- B. LẤY REALTIME TỪ REDIS (Code của bạn) ---
    redis_key = f"kline:{symbol}:{interval}" # Dùng format dấu : chuẩn
    raw_data = r.get(redis_key)
    
    # Fallback key cũ
    if not raw_data:
        raw_data = r.get(f"kline_{symbol}_{interval}")

    realtime_candle = None
    if raw_data:
        try:
            d = json.loads(raw_data)
            realtime_candle = {
                "timestamp": d.get("Open_time"), 
                "open": float(d.get("Open", 0)),
                "high": float(d.get("High", 0)),
                "low": float(d.get("Low", 0)),
                "close": float(d.get("Close", 0)),
                "volume": float(d.get("Volume", 0))
            }
        except Exception as e:
            print(f"Lỗi parse Redis JSON: {e}")

    # --- C. GỘP DỮ LIỆU ---
    final_data = history_data
    if realtime_candle:
        if len(final_data) > 0:
            # So sánh string timestamp
            if str(final_data[-1]['timestamp']) == str(realtime_candle['timestamp']):
                final_data[-1] = realtime_candle # Update nến cuối
            else:
                final_data.append(realtime_candle) # Thêm nến mới
        else:
            final_data.append(realtime_candle)

    return {"data": final_data}