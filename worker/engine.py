import time
import json
import redis
import os
import logging

# Cấu hình Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Kết nối Redis
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - ENGINE - %(message)s")

# Thay vì lưu vào SET, bạn phải lưu vào ZSET (Sorted Set) khi người dùng đặt lệnh:

def safe_float(v):
    try: return float(v)
    except: return 0.0

def get_order_details(order_id):
    """Lấy chi tiết lệnh từ Hash"""
    data = redis_client.hgetall(f"order:{order_id}")
    if not data: return None
    return {
        "order_id": data.get("order_id", order_id),
        "user_id": data.get("user_id"),
        "price": safe_float(data.get("price")),
        "amount": safe_float(data.get("amount")),
        "timestamp": safe_float(data.get("timestamp"))
    }

# Định nghĩa Lua Script: KHỚP LỆNH NỘI BỘ (P2P)
# Logic: Trừ tiền người mua, cộng tiền người bán, update/xóa order
LUA_MATCH_P2P = """
    local buy_oid = KEYS[1]
    local sell_oid = KEYS[2]
    local match_qty = tonumber(ARGV[1])
    local match_price = tonumber(ARGV[2])
    local buyer_id = ARGV[3]
    local seller_id = ARGV[4]

    local total_cost = match_qty * match_price

    -- 1. Chuyển tiền (Atomic Update)
    redis.call('HINCRBYFLOAT', 'user:'..buyer_id..':balance', 'usd', -total_cost)
    redis.call('HINCRBYFLOAT', 'user:'..buyer_id..':balance', 'btc', match_qty)
    redis.call('HINCRBYFLOAT', 'user:'..seller_id..':balance', 'usd', total_cost)
    redis.call('HINCRBYFLOAT', 'user:'..seller_id..':balance', 'btc', -match_qty)

    -- 2. Cập nhật Sổ lệnh (Giảm volume hoặc Xóa)
    -- (Logic chi tiết update volume order như đã bàn...)
    
    return "OK"
"""

# Định nghĩa Lua Script: KHỚP LỆNH VỚI DỮ LIỆU THẬT (REAL)
# Logic: Trừ tiền người mua, cộng coin (từ hư vô), KHÔNG cộng tiền cho ai cả (vào System)
LUA_MATCH_REAL = """
    local buy_oid = KEYS[1]
    local match_qty = tonumber(ARGV[1])
    local match_price = tonumber(ARGV[2])
    local buyer_id = ARGV[3]

    local total_cost = match_qty * match_price

    -- 1. Trừ tiền Buyer, Cộng Coin Buyer
    redis.call('HINCRBYFLOAT', 'user:'..buyer_id..':balance', 'usd', -total_cost)
    redis.call('HINCRBYFLOAT', 'user:'..buyer_id..':balance', 'btc', match_qty)

    -- 2. Ghi nhận doanh thu cho hệ thống (System Wallet)
    redis.call('HINCRBYFLOAT', 'system:wallet', 'usd', total_cost)

    -- 3. Cập nhật lệnh Buyer (Xóa hoặc giảm volume)
    -- KHÔNG CẦN CẬP NHẬT LỆNH BÁN (Vì là lệnh của Market Data)
    
    return "OK"
"""

# Đăng ký script với Redis
p2p_script_sha = redis_client.script_load(LUA_MATCH_P2P)
real_script_sha = redis_client.script_load(LUA_MATCH_REAL)


def run_engine():
    while True:
        # Lấy lệnh Mua tốt nhất của User
        best_buy_ids = redis_client.zrevrange("orderbook:virtual:buy", 0, 0)
        if not best_buy_ids: 
            time.sleep(0.5)
            continue
            
        best_buy = get_order_details(best_buy_ids[0])
        
        # --- LỚP 1: THỬ KHỚP NỘI BỘ (P2P) ---
        best_sell_ids = redis_client.zrange("orderbook:virtual:sell", 0, 0)
        match_found_p2p = False
        
        if best_sell_ids:
            best_sell = get_order_details(best_sell_ids[0])
            if best_buy['price'] >= best_sell['price']:
                # >>> GỌI LUA SCRIPT P2P Ở ĐÂY <<<
                amount = min(best_buy['amount'], best_sell['amount'])
                redis_client.evalsha(
                    p2p_script_sha, 
                    2, # Số lượng keys
                    best_buy['order_id'], best_sell['order_id'], # Keys
                    amount, best_sell['price'], best_buy['user_id'], best_sell['user_id'] # Args
                )
                match_found_p2p = True
                continue # Quay lại đầu vòng lặp ngay

        # --- LỚP 2: NẾU KHÔNG KHỚP NỘI BỘ -> KHỚP REAL ---
        if not match_found_p2p:
            # Lấy giá bán tốt nhất từ Binance (Market Data Worker đã nạp vào)
            real_asks = redis_client.zrange("orderbook:real_market:asks", 0, 0, withscores=True)
            
            if real_asks:
                real_price = float(real_asks[0][1]) # Giá (Score)
                
                # Nếu giá user đặt >= giá thị trường thật
                if best_buy['price'] >= real_price:
                     # >>> GỌI LUA SCRIPT REAL Ở ĐÂY <<<
                     # User chấp nhận mua giá thị trường, nhưng ta khớp giá real_price (lợi cho user)
                     redis_client.evalsha(
                        real_script_sha,
                        1, # Số lượng keys
                        best_buy['order_id'], # Key
                        best_buy['amount'], real_price, best_buy['user_id'] # Args
                     )
                     continue
        
        # Không khớp được ai cả -> Nghỉ tí
        time.sleep(0.1)

if __name__ == "__main__":
    run_engine()