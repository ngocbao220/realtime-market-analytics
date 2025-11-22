import time
import json
import redis
import os
import logging

# Cấu hình Redis (Lấy từ biến môi trường hoặc mặc định)
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Kết nối Redis
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
except Exception as e:
    print(f"Error connecting to Redis: {e}")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - ENGINE - %(message)s")

def safe_float(v):
    try: return float(v)
    except: return 0.0

# --- HÀM LẤY ORDER BOOK (Copy logic từ API nhưng dùng nội bộ) ---
def get_sorted_orders(side):
    redis_key = "orders:buy" if side == "buy" else "orders:sell"
    order_ids = redis_client.smembers(redis_key)
    
    if not order_ids:
        return []

    pipe = redis_client.pipeline()
    for oid in order_ids:
        pipe.hgetall(f"order:{oid}")
    results = pipe.execute()
    
    orders = []
    for data in results:
        if data and "order_id" in data and data["status"] == "pending":
            orders.append({
                "order_id": data["order_id"],
                "user_id": data["user_id"],
                "price": safe_float(data["price"]),
                "amount": safe_float(data["amount"]),
                "timestamp": safe_float(data["timestamp"])
            })

    # Sắp xếp: 
    # Buy: Giá CAO nhất trước. Nếu bằng giá, ai đến TRƯỚC (timestamp nhỏ) thì khớp trước.
    # Sell: Giá THẤP nhất trước.
    is_reverse = True if side == "buy" else False
    orders.sort(key=lambda x: (x["price"], -x["timestamp"] if is_reverse else x["timestamp"]), reverse=is_reverse)
    
    return orders

# --- HÀM XỬ LÝ GIAO DỊCH (MATCH) ---
def execute_trade(buy_order, sell_order):
    # 1. Xác định giá khớp & số lượng khớp
    # Quy tắc: Khớp theo giá của người đặt trước (Maker). 
    # Ở đây giả lập đơn giản: Khớp theo giá của lệnh Sell (hoặc Buy tùy logic). 
    # Để an toàn cho người mua, ta khớp giá Sell (vì Buy >= Sell, khớp giá Sell là có lợi cho Buyer).
    match_price = sell_order["price"]
    match_amount = min(buy_order["amount"], sell_order["amount"])
    
    logging.info(f"⚡ MATCH FOUND! {match_amount} BTC @ ${match_price}")
    logging.info(f"   Buyer: {buy_order['user_id']} | Seller: {sell_order['user_id']}")

    # 2. Cập nhật số dư (Balance) - QUAN TRỌNG
    total_cost = match_amount * match_price

    # Trừ tiền Buyer: (Đã trừ lúc đặt lệnh reserve? Ở đây làm đơn giản là trừ thẳng)
    # Cộng BTC cho Buyer
    redis_client.hincrbyfloat(f"user:{buy_order['user_id']}:balance", "usd", -total_cost)
    redis_client.hincrbyfloat(f"user:{buy_order['user_id']}:balance", "btc", match_amount)

    # Cộng tiền Seller
    # Trừ BTC Seller
    redis_client.hincrbyfloat(f"user:{sell_order['user_id']}:balance", "usd", total_cost)
    redis_client.hincrbyfloat(f"user:{sell_order['user_id']}:balance", "btc", -match_amount)

    # 3. Cập nhật Order Book
    
    # Giảm số lượng còn lại của lệnh
    buy_remaining = buy_order["amount"] - match_amount
    sell_remaining = sell_order["amount"] - match_amount

    # Update lệnh Buy
    if buy_remaining > 0.00000001: # Còn dư -> Update lại
        redis_client.hset(f"order:{buy_order['order_id']}", "amount", buy_remaining)
    else: # Hết -> Xóa khỏi list và đánh dấu status done
        redis_client.srem("orders:buy", buy_order["order_id"])
        redis_client.hset(f"order:{buy_order['order_id']}", "status", "filled")

    # Update lệnh Sell
    if sell_remaining > 0.00000001:
        redis_client.hset(f"order:{sell_order['order_id']}", "amount", sell_remaining)
    else:
        redis_client.srem("orders:sell", sell_order["order_id"])
        redis_client.hset(f"order:{sell_order['order_id']}", "status", "filled")

    # 4. Lưu lịch sử giao dịch (Trade History)
    trade_record = {
        "timestamp": time.time(),
        "price": match_price,
        "amount": match_amount,
        "buyer_id": buy_order["user_id"],
        "seller_id": sell_order["user_id"],
        "type": "buy_match" # hoặc sell_match
    }
    # Đẩy vào List để Dashboard hiển thị
    redis_client.lpush("trades:history", json.dumps(trade_record))
    # Giữ lại 100 giao dịch gần nhất thôi
    redis_client.ltrim("trades:history", 0, 99)

# --- VÒNG LẶP CHÍNH ---
def run_engine():
    logging.info("🚀 Matching Engine Started...")
    while True:
        try:
            # 1. Lấy sổ lệnh
            buy_orders = get_sorted_orders("buy")
            sell_orders = get_sorted_orders("sell")

            if not buy_orders or not sell_orders:
                time.sleep(1) # Không có lệnh thì nghỉ 1s cho đỡ tốn CPU
                continue

            # 2. So sánh cặp đầu tiên (Best Bid vs Best Ask)
            best_buy = buy_orders[0]
            best_sell = sell_orders[0]

            # Điều kiện khớp: Giá mua >= Giá bán
            if best_buy["price"] >= best_sell["price"]:
                execute_trade(best_buy, best_sell)
                # Không sleep, chạy tiếp ngay để khớp nốt số dư nếu có
            else:
                # Giá chưa gặp nhau -> Nghỉ xíu
                time.sleep(1)

        except Exception as e:
            logging.error(f"Engine Crash: {e}")
            time.sleep(5)

if __name__ == "__main__":
    run_engine()