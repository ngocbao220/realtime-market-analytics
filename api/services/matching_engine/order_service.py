import uuid
import time
from db import redis_client

# --- HÀM HỖ TRỢ ---
def safe_float(v):
    try: return float(v)
    except: return 0.0

# --- 1. TẠO LỆNH MỚI (PLACE ORDER) ---
def place_order(user_id: str, side: str, price: float, amount: float):
    """
    side: 'buy' hoặc 'sell'
    """
    # Tạo ID duy nhất cho lệnh
    order_id = str(uuid.uuid4())
    timestamp = int(time.time())
    
    # Chuẩn bị dữ liệu
    order_data = {
        "order_id": order_id,
        "user_id": user_id,
        "side": side, # Lưu thêm side vào trong data để dễ check
        "price": price,
        "amount": amount,
        "status": "pending", # open, filled, cancelled
        "timestamp": timestamp
    }

    # 1. Lưu chi tiết lệnh vào Redis Hash
    redis_client.hset(f"order:{order_id}", mapping=order_data)

    # 2. Đưa ID vào danh sách tương ứng (orders:buy hoặc orders:sell)
    # Dùng SADD (Set) để đảm bảo không trùng lặp
    redis_key = "orders:buy" if side == "buy" else "orders:sell"
    redis_client.sadd(redis_key, order_id)

    return order_data

# --- 2. LẤY ORDER BOOK (DANH SÁCH LỆNH) ---
def get_orderbook(side: str):
    """
    Lấy danh sách lệnh theo chiều buy hoặc sell và sắp xếp giá
    """
    redis_key = "orders:buy" if side == "buy" else "orders:sell"
    
    # 1. Lấy tất cả order_id trong danh sách
    order_ids = redis_client.smembers(redis_key)
    
    if not order_ids:
        return []

    # 2. Dùng Pipeline để lấy chi tiết từng lệnh (Tối ưu tốc độ)
    pipe = redis_client.pipeline()
    for oid in order_ids:
        pipe.hgetall(f"order:{oid}")
    
    results = pipe.execute()
    
    orders = []
    for data in results:
        if data and "order_id" in data:
            orders.append({
                "order_id": data["order_id"],
                "user_id": data["user_id"],
                "side": data.get("side", side),
                "price": safe_float(data["price"]),
                "amount": safe_float(data["amount"]),
                "status": data["status"],
                "timestamp": int(data["timestamp"])
            })

    # 3. SẮP XẾP ORDER BOOK (QUAN TRỌNG)
    # - Buy: Giá CAO nhất xếp trên cùng (Reverse = True)
    # - Sell: Giá THẤP nhất xếp trên cùng (Reverse = False)
    is_reverse = True if side == "buy" else False
    
    # Sort theo giá (price)
    orders.sort(key=lambda x: x["price"], reverse=is_reverse)
    
    return orders

# --- 3. HỦY LỆNH (Optional) ---
def cancel_order(order_id: str):
    # Lấy info để biết side nào mà xóa khỏi list
    data = redis_client.hgetall(f"order:{order_id}")
    if not data:
        return False
    
    side = data.get("side")
    redis_key = "orders:buy" if side == "buy" else "orders:sell"
    
    # Xóa khỏi danh sách và xóa data
    redis_client.srem(redis_key, order_id)
    redis_client.delete(f"order:{order_id}")
    return True