from db import r, ch_client
import time
import json
from typing import Dict, Any

def place_order_logic(user_id, side, price, amount):
    """
    Logic đặt lệnh: Kiểm tra số dư và cập nhật Redis
    """
    user_key = f"user:{user_id}"
    if not r.exists(user_key):
        return {"status": "failed", "detail": "User không tồn tại"}
    
    user_data = r.hgetall(user_key)
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
            r.hset(user_key, mapping={"usd": new_usd, "btc": new_btc})
            return {"status": "success", "order_id": int(time.time()), "message": "Mua thành công"}
        else:
            return {"status": "failed", "detail": "Số dư USD không đủ"}
    elif side == "sell":
        if current_btc >= amount:
            new_usd = current_usd + total_cost
            new_btc = current_btc - amount
            r.hset(user_key, mapping={"usd": new_usd, "btc": new_btc})
            return {"status": "success", "order_id": int(time.time()), "message": "Bán thành công"}
        else:
            return {"status": "failed", "detail": "Số dư BTC không đủ"}
            
    return {"status": "failed", "detail": "Lỗi tham số"}

def get_orderbook_data(symbol: str) -> Dict[str, Any]:
    """
    Lấy dữ liệu Orderbook: Ưu tiên Redis, Fallback về ClickHouse
    """
    symbol = symbol.upper()
    
    # 1. Thử lấy từ REDIS (Nhanh nhất)
    redis_key = f"orderbook:{symbol}"
    raw_data = r.get(redis_key)
    
    if raw_data:
        try:
            d = json.loads(raw_data)
            # Format dữ liệu từ Redis JSON
            bids = [[str(p), str(q)] for p, q in zip(d.get("Bid_prices",[]), d.get("Bid_quantities",[]))]
            asks = [[str(p), str(q)] for p, q in zip(d.get("Ask_prices",[]), d.get("Ask_quantities",[]))]
            
            return {
                "symbol": symbol,
                "source": "redis",
                "timestamp": d.get("Event_time", ""),
                "bids": bids[:10], 
                "asks": asks[:10]
            }
        except Exception as e:
            print(f"Redis orderbook parse error: {e}")
            # Nếu lỗi parse, để code chạy tiếp xuống phần ClickHouse (fallback)

    # 2. Fallback: Lấy Snapshot từ CLICKHOUSE (Nếu Redis ko có)
    try:
        query = f"""
        SELECT 
            event_time,
            bid_prices,
            bid_quantities,
            ask_prices,
            ask_quantities
        FROM orderbook
        WHERE symbol = '{symbol}'
        ORDER BY event_time DESC
        LIMIT 1
        """
        result = ch_client.execute(query)
        
        if result:
            row = result[0] # Lấy dòng đầu tiên
            
            # Format dữ liệu từ ClickHouse (Arrays)
            # row[1] là bid_prices, row[2] là bid_quantities
            bids = [
                [str(p), str(q)] 
                for p, q in zip(row[1], row[2])
            ][:10]
            
            asks = [
                [str(p), str(q)] 
                for p, q in zip(row[3], row[4])
            ][:10]
            
            timestamp_str = row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0])

            return {
                "symbol": symbol,
                "source": "clickhouse",
                "timestamp": timestamp_str,
                "bids": bids,
                "asks": asks
            }
            
    except Exception as e:
        print(f"ClickHouse orderbook error: {e}")

    # 3. Nếu cả 2 đều tạch -> Trả về rỗng
    return {"bids": [], "asks": [], "symbol": symbol, "message": "No data"}