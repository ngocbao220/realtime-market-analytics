import uuid
import time
from db import redis_client  # Đảm bảo bạn đã config redis_client ở file db.py

# --- HÀM HỖ TRỢ ---
def safe_float(v):
    try: return float(v)
    except: return 0.0

# --- 1. TẠO LỆNH MỚI (PLACE ORDER) ---
def place_order(user_id: str, symbol: str, side: str, price: float, amount: float):
    """
    side: Input là 'bids'/'asks'.
    """
    # 1. Chuẩn hóa side (buy -> bids, sell -> asks)
    if side in ['buy', 'bid']:
        side_key = 'bids'
    elif side in ['sell', 'ask']:
        side_key = 'asks'
    else:
        side_key = side # Mặc định nếu đã đúng

    # 2. Tạo ID và Timestamp
    order_id = str(uuid.uuid4())
    timestamp = time.time() # Dùng float để chính xác mili-giây
    
    # 3. Chuẩn bị dữ liệu chi tiết
    order_data = {
        "order_id": order_id,
        "user_id": user_id,
        "symbol": symbol,
        "side": side_key,
        "price": price,
        "amount": amount,
        "status": "pending", 
        "timestamp": timestamp
    }

    # 4. THỰC HIỆN GHI REDIS (Pipeline để Atomic)
    pipe = redis_client.pipeline()

    # a. Lưu chi tiết lệnh vào Hash
    pipe.hset(f"order:{order_id}", mapping=order_data)

    # b. Đưa vào Sổ lệnh Virtual (ZSET)
    # Key: orderbook:virtual:{symbol}:{bids/asks}
    # Score: PRICE (Để matching engine tìm giá tốt nhất nhanh nhất)
    # Member: order_id
    zset_key = f"orderbook:virtual:{symbol}:{side_key}"
    pipe.zadd(zset_key, {order_id: price})

    pipe.execute()
    
    return order_data

# --- 2. LẤY ORDER BOOK (DANH SÁCH LỆNH) ---
def get_orderbook(symbol: str, side: str):
    """
    Lấy danh sách lệnh để hiển thị hoặc để Matching Engine quét.
    """
    # Chuẩn hóa side
    side_key = 'bids' if side in ['buy', 'bid'] else 'asks'
    
    zset_key = f"orderbook:virtual:{symbol}:{side_key}"
    
    # 1. Lấy danh sách ID từ ZSET
    # - Nếu là BIDS (Người mua): Cần giá CAO nhất xếp trước -> ZREVRANGE
    # - Nếu là ASKS (Người bán): Cần giá THẤP nhất xếp trước -> ZRANGE
    if side_key == 'bids':
        order_ids = redis_client.zrevrange(zset_key, 0, -1)
    else:
        order_ids = redis_client.zrange(zset_key, 0, -1)
    
    if not order_ids:
        return []

    # 2. Pipeline lấy chi tiết từng lệnh
    pipe = redis_client.pipeline()
    for oid in order_ids:
        pipe.hgetall(f"order:{oid}")
    
    results = pipe.execute()
    
    orders = []
    for data in results:
        # Kiểm tra data rác (có trong list nhưng mất trong detail)
        if data and "order_id" in data:
            # Format lại dữ liệu cho chuẩn kiểu số
            formatted_order = {
                "order_id": data["order_id"],
                "user_id": data["user_id"],
                "price": safe_float(data["price"]),
                "amount": safe_float(data["amount"]),
                "timestamp": safe_float(data["timestamp"]),
                "status": data["status"]
            }
            orders.append(formatted_order)

    # Lưu ý: ZSET đã sắp xếp theo Giá rồi, nhưng nếu có nhiều lệnh CÙNG GIÁ
    # ta nên sort lại bằng Python theo timestamp (Ai đến trước khớp trước - FIFO)
    # Logic: Sort ổn định (Stable sort) theo timestamp
    if orders:
        orders.sort(key=lambda x: x["timestamp"]) # Sắp xếp tăng dần theo thời gian (Cũ nhất lên đầu)
        # Vì Python sort là stable, nó sẽ giữ nguyên thứ tự giá (đã sort bởi Redis)
        # và chỉ đổi chỗ những ông cùng giá dựa theo thời gian.
        
        # Tuy nhiên, để chính xác tuyệt đối:
        # Bids: Giá Cao -> Thấp. Cùng giá: Thời gian Cũ -> Mới.
        if side_key == 'bids':
            orders.sort(key=lambda x: (-x["price"], x["timestamp"]))
        else:
            orders.sort(key=lambda x: (x["price"], x["timestamp"]))

    return orders

# --- 3. HỦY LỆNH ---
def cancel_order(order_id: str):
    # 1. Lấy thông tin lệnh để biết nó thuộc symbol nào, side nào
    order_key = f"order:{order_id}"
    data = redis_client.hgetall(order_key)
    
    if not data:
        return False # Không tìm thấy lệnh
    
    symbol = data.get("symbol")
    side = data.get("side") # bids hoặc asks
    
    zset_key = f"orderbook:virtual:{symbol}:{side}"
    
    # 2. Xóa khỏi ZSET và Xóa chi tiết (Atomic)
    pipe = redis_client.pipeline()
    pipe.zrem(zset_key, order_id)
    pipe.delete(order_key)
    pipe.execute()
    
    return True