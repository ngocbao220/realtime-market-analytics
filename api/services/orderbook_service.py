from db import redis_client, ch_client
import time
import json
from typing import Dict, Any, Union, List

def place_order_logic(user_id, side, price, amount):
    """
    Logic đặt lệnh: Kiểm tra số dư và cập nhật Redis
    """
    user_key = f"user:{user_id}"
    if not redis_client.exists(user_key):
        return {"status": "failed", "detail": "User không tồn tại"}
    
    user_data = redis_client.hgetall(user_key)
    # Chuyển đổi an toàn sang float
    try:
        current_usd = float(user_data.get("usd", 0))
        current_btc = float(user_data.get("btc", 0))
    except (ValueError, TypeError):
        current_usd = 0.0
        current_btc = 0.0
        
    total_cost = price * amount

    if side == "buy":
        if current_usd >= total_cost:
            new_usd = current_usd - total_cost
            new_btc = current_btc + amount
            redis_client.hset(user_key, mapping={"usd": new_usd, "btc": new_btc})
            return {"status": "success", "order_id": int(time.time()), "message": "Mua thành công"}
        else:
            return {"status": "failed", "detail": "Số dư USD không đủ"}
    elif side == "sell":
        if current_btc >= amount:
            new_usd = current_usd + total_cost
            new_btc = current_btc - amount
            redis_client.hset(user_key, mapping={"usd": new_usd, "btc": new_btc})
            return {"status": "success", "order_id": int(time.time()), "message": "Bán thành công"}
        else:
            return {"status": "failed", "detail": "Số dư BTC không đủ"}
            
    return {"status": "failed", "detail": "Lỗi tham số"}

def get_orderbook_data(symbol: str, type: str = "real_market", side: str = "both") -> Union[List, Dict]:
    """
    Lấy dữ liệu Orderbook từ Redis ZSET.
    Args:
        symbol: BTCUSDT
        type: "real_market" (hoặc virtual)
        side: "bids", "asks", hoặc "both"
    """
    symbol = symbol.upper()
    
    # Hàm nội bộ để lấy và parse dữ liệu từ Redis ZSET
    def fetch_and_parse(key_side, reverse_sort=True):
        # Key format chính xác: orderbook:real_market:BTCUSDT:bids
        key = f"orderbook:{type}:{symbol}:{key_side}"
        
        # Lấy 50 phần tử mới nhất (theo score/timestamp)
        # Redis ZSET lưu: member='{"p":..., "a":...}', score=timestamp
        items = redis_client.zrange(key, 0, 50, desc=True)
        
        orders = []
        for item in items:
            try:
                d = json.loads(item)
                orders.append({
                    "price": float(d.get("p", 0)),
                    "amount": float(d.get("a", 0))
                })
            except: continue
            
        # Sắp xếp lại theo Giá (Price)
        # Bids: Giá cao nhất lên đầu (Reverse=True)
        # Asks: Giá thấp nhất lên đầu (Reverse=False)
        return sorted(orders, key=lambda x: x["price"], reverse=reverse_sort)[:20]

    result = {}

    # 1. Xử lý Bids (Mua)
    if side == "bids" or side == "both":
        bids_data = fetch_and_parse("bids", reverse_sort=True)
        # Format đơn giản: [[price, amount], ...]
        formatted_bids = [[o["price"], o["amount"]] for o in bids_data]
        
        if side == "bids": 
            return formatted_bids # Trả về List nếu chỉ hỏi bids
        
        result["bids"] = formatted_bids

    # 2. Xử lý Asks (Bán)
    if side == "asks" or side == "both":
        asks_data = fetch_and_parse("asks", reverse_sort=False)
        formatted_asks = [[o["price"], o["amount"]] for o in asks_data]
        
        if side == "asks": 
            return formatted_asks # Trả về List nếu chỉ hỏi asks
        
        result["asks"] = formatted_asks

    # Trả về Dict nếu hỏi cả hai
    result["symbol"] = symbol
    result["type"] = type
    result["timestamp"] = int(time.time() * 1000)
    return result