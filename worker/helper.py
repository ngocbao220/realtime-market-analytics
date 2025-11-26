import json
from config import redis_client

# Lấy thông tin chi tiết của một order dựa vào order_id
def get_order_details(order_id):
    """Helper lấy thông tin lệnh"""
    data = redis_client.hgetall(f"order:virtual:{order_id}")
    if not data: return None
    # Convert số
    for k in ['price', 'amount', 'remaining_amount']:
        if k in data: data[k] = float(data[k])
    return data

# Lưu thông tin trades
def save_trade_log(pipe, symbol, price, amount, taker_side, buyer_id, seller_id, timestamp):
    """
    Hàm lưu lịch sử giao dịch tập trung (Chỉ lưu vào List JSON, bỏ qua Hash).
    """
    # 1. Ghi vào Lịch sử thị trường (Market Trades)
    # Key: trades:virtual:BTCUSDT
    # Lưu JSON trực tiếp để Frontend hiển thị luôn
    market_trade_data = {
        "price": price,
        "amount": amount,
        "side": taker_side, # 'buy' hoặc 'sell' (phe chủ động)
        "time": timestamp   # timestamp float
    }
    market_key = f"trades:virtual:{symbol}"
    pipe.lpush(market_key, json.dumps(market_trade_data))
    pipe.ltrim(market_key, 0, 99) # Giữ 100 trade mới nhất

    # 2. Ghi vào Lịch sử User (My Trades)
    # A. Ghi cho người MUA
    buyer_rec = {
        "symbol": symbol, 
        "price": price, 
        "amount": amount,
        "side": "buy", 
        "role": "taker" if taker_side == "buy" else "maker",
        "time": timestamp
    }
    # Key: user:1:trades hoặc user:3:trades
    buyer_key = f"user:{buyer_id}:trades"
    pipe.lpush(buyer_key, json.dumps(buyer_rec))
    # QUAN TRỌNG: Phải cắt ngắn list, đặc biệt là với System vì nó trade rất nhiều
    pipe.ltrim(buyer_key, 0, 499) # Giữ 500 trade gần nhất cho mỗi user

    # B. Ghi cho người BÁN
    seller_rec = {
        "symbol": symbol, 
        "price": price, 
        "amount": amount,
        "side": "sell", 
        "role": "taker" if taker_side == "sell" else "maker",
        "time": timestamp
    }
    seller_key = f"user:{seller_id}:trades"
    pipe.lpush(seller_key, json.dumps(seller_rec))
    pipe.ltrim(seller_key, 0, 499)

# Lưu thông tin lịch sử đặt lệnh (khi lệnh khớp hoặc bị huỷ lệnh)
def save_order_history(pipe, order_id, user_id, symbol, side, price, amount, filled_qty, status, timestamp):
    """
    Ghi log lịch sử thay đổi trạng thái lệnh.
    Dùng để hiển thị tab "Order History" trên frontend.
    """
    # Key List: user:{id}:order_history
    history_key = f"user:{user_id}:order_history"
    
    record = {
        "order_id": order_id,
        "symbol": symbol,
        "side": side,
        "type": "LIMIT", # Giả định limit
        "price": price,
        "amount": amount,
        "filled": filled_qty, # Số lượng vừa khớp thêm (delta) hoặc tổng đã khớp
        "status": status,     # FILLED, PARTIALLY_FILLED, CANCELLED
        "time": timestamp
    }
    
    pipe.lpush(history_key, json.dumps(record))
    pipe.ltrim(history_key, 0, 199) # Giữ 200 lệnh gần nhất
