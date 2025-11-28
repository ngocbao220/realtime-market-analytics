import uuid
import time
import json
from db import redis_client

KEY_USER_OPEN_ORDERS = "user:{}:open_orders"
KEY_ORDER_DETAIL = "order:virtual:{}" 

# --- KEYS FORMAT ---
# Để Lua script dễ xử lý, ta quy định format key rõ ràng
# KEYS[1]: User Balance Hash
# KEYS[2]: Order Detail Hash
# KEYS[3]: Orderbook ZSet
# KEYS[4]: User Open Orders Set

# --- LUA SCRIPT: PLACE ORDER ---
LUA_PLACE_ORDER = """
    local user_bal_key = KEYS[1]
    local order_detail_key = KEYS[2]
    local orderbook_key = KEYS[3]
    local user_open_orders_key = KEYS[4]

    local side = ARGV[1] -- 'bids' or 'asks'
    local price = tonumber(ARGV[2])
    local amount = tonumber(ARGV[3])
    local total_cost = price * amount
    local order_id = ARGV[4]
    local timestamp = ARGV[5]
    local symbol = ARGV[6]
    local user_id = ARGV[7]

    -- 1. KIỂM TRA SỐ DƯ
    local avail_usd = tonumber(redis.call('HGET', user_bal_key, 'usd') or 0)
    local avail_btc = tonumber(redis.call('HGET', user_bal_key, 'btc') or 0)

    if side == 'bids' then
        if avail_usd < total_cost then
            return {err="Số dư USD không đủ"}
        end
        -- Trừ USD, Cộng Reserved USD
        redis.call('HINCRBYFLOAT', user_bal_key, 'usd', -total_cost)
        redis.call('HINCRBYFLOAT', user_bal_key, 'reserved_usd', total_cost)
    else
        if avail_btc < amount then
            return {err="Số dư Coin không đủ"}
        end
        -- Trừ BTC, Cộng Reserved BTC
        redis.call('HINCRBYFLOAT', user_bal_key, 'btc', -amount)
        redis.call('HINCRBYFLOAT', user_bal_key, 'reserved_btc', amount)
    end

    -- 2. TẠO LỆNH (Hash)
    redis.call('HSET', order_detail_key,
        'order_id', order_id,
        'user_id', user_id,
        'symbol', symbol,
        'side', side,
        'type', 'LIMIT',
        'price', price,
        'amount', amount,
        'filled_amount', 0,
        'remaining_amount', amount,
        'status', 'NEW',
        'timestamp_created', timestamp
    )

    -- 3. CẬP NHẬT ORDERBOOK & INDEX
    redis.call('ZADD', orderbook_key, price, order_id)
    redis.call('SADD', user_open_orders_key, order_id)

    return {ok="OK"}
"""

# --- LUA SCRIPT: CANCEL ORDER ---
# Script này quan trọng để tránh Race Condition:
# (Ví dụ: User bấm hủy đúng lúc lệnh vừa khớp xong -> Hủy lệnh đã khớp -> Sai tiền)
LUA_CANCEL_ORDER = """
    local order_key = KEYS[1]
    local orderbook_key = KEYS[2]
    local user_open_orders_key = KEYS[3]
    local user_bal_key = KEYS[4]

    local order_id = ARGV[1]
    local user_id = ARGV[2]

    -- 1. CHECK QUYỀN VÀ TRẠNG THÁI
    local exists = redis.call('EXISTS', order_key)
    if exists == 0 then return {err="Lệnh không tồn tại"} end

    local owner = redis.call('HGET', order_key, 'user_id')
    if owner ~= user_id then return {err="Không chính chủ"} end

    local status = redis.call('HGET', order_key, 'status')
    if status == 'FILLED' or status == 'CANCELLED' then
        return {err="Không thể hủy (Lệnh đã khớp hoặc đã hủy)"}
    end

    -- 2. LẤY THÔNG TIN HOÀN TIỀN
    local remaining = tonumber(redis.call('HGET', order_key, 'remaining_amount') or 0)
    local price = tonumber(redis.call('HGET', order_key, 'price') or 0)
    local side = redis.call('HGET', order_key, 'side')

    if remaining <= 0 then return {err="Lệnh đã khớp hết"} end

    -- 3. THỰC HIỆN HỦY
    redis.call('ZREM', orderbook_key, order_id)
    redis.call('SREM', user_open_orders_key, order_id)
    redis.call('HSET', order_key, 'status', 'CANCELLED')
    redis.call('HSET', order_key, 'amount', 0) -- Set về 0 để an toàn

    -- 4. HOÀN TIỀN
    if side == 'bids' then
        local refund = remaining * price
        redis.call('HINCRBYFLOAT', user_bal_key, 'reserved_usd', -refund)
        redis.call('HINCRBYFLOAT', user_bal_key, 'usd', refund)
    else
        local refund = remaining
        redis.call('HINCRBYFLOAT', user_bal_key, 'reserved_btc', -refund)
        redis.call('HINCRBYFLOAT', user_bal_key, 'btc', refund)
    end

    return {ok="Đã hủy thành công"}
"""

# Load scripts 1 lần khi khởi động app
try:
    place_order_sha = redis_client.script_load(LUA_PLACE_ORDER)
    cancel_order_sha = redis_client.script_load(LUA_CANCEL_ORDER)
except:
    pass # Xử lý log lỗi nếu cần

# --- HÀM PYTHON GỌI LUA ---
def place_virtual_order(user_id: str, symbol: str, side: str, price: float, amount: float):
    # Chuẩn hóa input
    side = 'bids' if side in ['buy', 'bid'] else 'asks'
    symbol = symbol.upper()
    order_id = str(uuid.uuid4())
    timestamp = time.time()

    try:
        # Gọi Lua Script
        # Keys: [UserBal, OrderDetail, Orderbook, UserOpenOrders]
        # Args: [side, price, amount, order_id, timestamp, symbol, user_id]
        res = redis_client.evalsha(
            place_order_sha,
            4, # numkeys
            f"user:{user_id}:balance",
            f"order:virtual:{order_id}",
            f"orderbook:virtual:{symbol}:{side}",
            f"user:{user_id}:open_orders",
            side, price, amount, order_id, timestamp, symbol, user_id
        )

        # Lua trả về dict, ví dụ {ok="OK"} hoặc {err="Lỗi..."}
        # redis-py có thể trả về bytes hoặc string tùy decode_responses
        if res and 'err' in res:
             return {"success": False, "msg": res['err']}
        
        return {"success": True, "msg": "Đặt lệnh thành công", "order_id": order_id}

    except Exception as e:
        return {"success": False, "msg": f"System Error: {str(e)}"}

def save_order_history(pipe, order_id, user_id, symbol, side, price, amount, filled_qty, status, timestamp):
    """
    Ghi log trạng thái lệnh vào danh sách lịch sử user.
    Key: user:{id}:order_history
    """
    history_key = f"user:{user_id}:order_history"
    
    record = {
        "order_id": order_id,
        "symbol": symbol,
        "side": side,
        "type": "LIMIT",
        "price": float(price),
        "amount": float(amount),
        "filled": float(filled_qty), # Số lượng đã khớp được trước khi hủy
        "status": status,            # Ở đây sẽ là "CANCELLED"
        "time": timestamp
    }
    
    pipe.lpush(history_key, json.dumps(record))
    pipe.ltrim(history_key, 0, 199) # Giữ 200 log gần nhất

# --- HÀM HỦY LỆNH CHÍNH ---
def cancel_virtual_order(user_id: str, order_id: str):
    order_key = f"order:virtual:{order_id}"
    
    # 1. Lấy thông tin chi tiết lệnh (Cần lấy thêm Price, Amount, Filled để ghi log)
    # Fields: symbol, side, price, amount, filled_amount
    fields = ["symbol", "side", "price", "amount", "filled_amount"]
    meta = redis_client.hmget(order_key, fields)
    
    # Kiểm tra dữ liệu
    if not meta[0]: 
        return {"success": False, "msg": "Lệnh không tồn tại"}
    
    symbol = meta[0]
    side = meta[1]
    price = float(meta[2] or 0)
    amount = float(meta[3] or 0)
    filled_amount = float(meta[4] or 0)

    if price <= 0:
        return {"success": False, "msg": "Giá đặt lệnh phải lớn hơn 0"}
    if amount <= 0:
        return {"success": False, "msg": "Số lượng phải lớn hơn 0"}
    
    try:
        # 2. Gọi Lua Script để Hủy và Hoàn tiền (Atomic)
        res = redis_client.evalsha(
            cancel_order_sha,
            4,
            order_key,                              # KEYS[1]
            f"orderbook:virtual:{symbol}:{side}",   # KEYS[2]
            f"user:{user_id}:open_orders",          # KEYS[3]
            f"user:{user_id}:balance",              # KEYS[4]
            order_id, user_id                       # ARGV
        )

        # Kiểm tra lỗi từ Lua
        if res and 'err' in res:
             return {"success": False, "msg": res['err']}
        
        # 3. GHI LOG ORDER HISTORY (Nếu Lua chạy thành công)
        # Lúc này tiền đã về ví, lệnh đã xóa khỏi sổ lệnh.
        # Ta ghi nhận trạng thái cuối cùng là CANCELLED.
        pipe = redis_client.pipeline()
        
        save_order_history(
            pipe=pipe,
            order_id=order_id,
            user_id=user_id,
            symbol=symbol,
            side=side,
            price=price,
            amount=amount,
            filled_qty=filled_amount, # Giữ nguyên số lượng đã khớp (nếu có)
            status="CANCELLED",
            timestamp=time.time()
        )
        
        pipe.execute()
        
        return {"success": True, "msg": "Hủy lệnh thành công"}

    except Exception as e:
        return {"success": False, "msg": f"System Error: {str(e)}"}
    
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
                # Check status
                order_status = order_data.get("status")
                if order_status not in ["FILLED", "CANCELLED"]:
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

def get_user_order_history(user_id: str, limit: int = 50):
    """
    Lấy lịch sử đặt lệnh (Đã khớp, Đã hủy) của User.
    Key: user:{id}:order_history (List JSON)
    """
    history_key = f"user:{user_id}:order_history"
    
    # Lấy danh sách từ Redis (List)
    raw_list = redis_client.lrange(history_key, 0, limit - 1)
    
    history = []
    for item in raw_list:
        try:
            # Redis trả về bytes hoặc string, cần decode và parse JSON
            data = json.loads(item)
            history.append(data)
        except Exception as e:
            continue
            
    return history