import uuid
import time
import redis
from db import redis_client

# --- KEYS ---
KEY_ORDER_DETAIL = "order:virtual:{}"         # Hash: Chi tiết lệnh
KEY_ORDERBOOK = "orderbook:virtual:{}:{}"     # ZSet: orderbook:virtual:BTCUSDT:bids
KEY_USER_OPEN_ORDERS = "user:{}:open_orders"  # Set: Danh sách lệnh đang treo của user
KEY_USER_BALANCE = "user:{}:balance"          # Hash: Balance

def place_virtual_order(user_id: str, symbol: str, side: str, price: float, amount: float):
    """
    Đặt lệnh Virtual:
    1. Check số dư.
    2. Trừ tiền khả dụng -> Cộng tiền đóng băng (Reserved).
    3. Lưu lệnh vào Redis Orderbook.
    """
    # Chuẩn hóa input
    side = 'bids' if side in ['buy', 'bid'] else 'asks'
    symbol = symbol.upper()
    price = float(price)
    amount = float(amount)
    total_cost = price * amount
    
    user_bal_key = KEY_USER_BALANCE.format(user_id)
    
    # 1. KIỂM TRA SỐ DƯ (Optimistic Locking với WATCH)
    # Trong thực tế production nên dùng Lua Script, ở đây dùng Watch cho dễ hiểu
    with redis_client.pipeline() as pipe:
        while True:
            try:
                pipe.watch(user_bal_key)
                balance_data = pipe.hgetall(user_bal_key)
                
                avail_usd = float(balance_data.get("usd", 0))
                avail_btc = float(balance_data.get("btc", 0))

                # Check đủ tiền không
                if side == 'bids': # Mua -> Cần USD
                    if avail_usd < total_cost:
                        return {"success": False, "msg": "Số dư USD không đủ"}
                else: # Bán -> Cần BTC
                    if avail_btc < amount:
                        return {"success": False, "msg": "Số dư BTC không đủ"}

                # 2. THỰC HIỆN TRANSACTION
                pipe.multi() 
                
                # A. Trừ tiền & Đóng băng
                if side == 'bids':
                    pipe.hincrbyfloat(user_bal_key, "usd", -total_cost)
                    pipe.hincrbyfloat(user_bal_key, "reserved_usd", total_cost)
                else:
                    pipe.hincrbyfloat(user_bal_key, "btc", -amount)
                    pipe.hincrbyfloat(user_bal_key, "reserved_btc", amount)

                # B. Tạo Lệnh
                order_id = str(uuid.uuid4())
                timestamp = time.time()
                order_data = {
                    "order_id": order_id, "user_id": user_id,
                    "symbol": symbol, "side": side, "type": "LIMIT",
                    "price": price, "amount": amount, 
                    "filled_amount": 0.0, "remaining_amount": amount,
                    "status": "NEW", "timestamp_created": timestamp
                }
                pipe.hset(KEY_ORDER_DETAIL.format(order_id), mapping=order_data)

                # C. Đưa vào Sổ lệnh (ZSET: Score = Price)
                zset_key = KEY_ORDERBOOK.format(symbol, side)
                pipe.zadd(zset_key, {order_id: price})

                # D. Index vào danh sách lệnh của User
                pipe.sadd(KEY_USER_OPEN_ORDERS.format(user_id), order_id)

                pipe.execute()
                return {"success": True, "msg": "Đặt lệnh thành công", "order_id": order_id}

            except redis.WatchError:
                # Nếu số dư bị thay đổi bởi luồng khác giữa chừng, retry
                continue
            except Exception as e:
                return {"success": False, "msg": str(e)}

def cancel_virtual_order(user_id: str, order_id: str):
    """
    Hủy lệnh: Hoàn tiền từ Reserved về Available
    """
    order_key = KEY_ORDER_DETAIL.format(order_id)
    order_data = redis_client.hgetall(order_key)
    
    if not order_data: return {"success": False, "msg": "Lệnh không tồn tại"}
    if order_data.get("user_id") != str(user_id): return {"success": False, "msg": "Không chính chủ"}
    if order_data.get("status") in ["FILLED", "CANCELLED"]: return {"success": False, "msg": "Không thể hủy"}

    symbol = order_data["symbol"]
    side = order_data["side"]
    price = float(order_data["price"])
    remaining = float(order_data["remaining_amount"])

    if remaining <= 0: return {"success": False, "msg": "Lệnh đã khớp hết"}

    pipe = redis_client.pipeline()
    
    # 1. Xóa khỏi Orderbook và Index
    pipe.zrem(KEY_ORDERBOOK.format(symbol, side), order_id)
    pipe.srem(KEY_USER_OPEN_ORDERS.format(user_id), order_id)
    
    # 2. Update Status
    pipe.hset(order_key, "status", "CANCELLED")
    
    # 3. Hoàn tiền
    user_bal_key = KEY_USER_BALANCE.format(user_id)
    if side == "bids":
        refund = remaining * price
        pipe.hincrbyfloat(user_bal_key, "reserved_usd", -refund)
        pipe.hincrbyfloat(user_bal_key, "usd", refund)
    else:
        refund = remaining
        pipe.hincrbyfloat(user_bal_key, "reserved_btc", -refund)
        pipe.hincrbyfloat(user_bal_key, "btc", refund)

    pipe.execute()
    return {"success": True, "msg": "Hủy lệnh thành công"}

def get_user_open_orders(user_id: str):
    """
    Get list of user's open orders (NEW/PARTIAL)
    """
    # Key containing list of order IDs: user:{id}:open_orders
    user_orders_key = KEY_USER_OPEN_ORDERS.format(user_id)
    
    # 1. Get all Order IDs in Set
    order_ids = redis_client.smembers(user_orders_key)
    
    orders = []
    for oid in order_ids:
        # 2. Get details of each order
        order_data = redis_client.hgetall(KEY_ORDER_DETAIL.format(oid))
        if order_data:
            try:
                # Format return data
                orders.append({
                    "order_id": order_data.get("order_id"),
                    "symbol": order_data.get("symbol"),
                    "side": order_data.get("side"), # bids/asks
                    "price": float(order_data.get("price", 0)),
                    "amount": float(order_data.get("amount", 0)),
                    "filled": float(order_data.get("filled_amount", 0)),
                    "status": order_data.get("status"),
                    "time": float(order_data.get("timestamp_created", 0))
                })
            except: continue
            
    # 3. Sort: Newest orders first
    return sorted(orders, key=lambda x: x["time"], reverse=True)
