"""
Service layer cho trade data và price history
"""

from typing import Dict, Any, List
from db import ch_client, redis_client
from config import INTERVAL_MAP
import json
from datetime import datetime

def get_trades(symbol: str, mode="real_time", type="real", user_id=None, limit=None):
    """
    Dùng để lấy lịch sử giao dịch khớp.
    mode: "real_time" or "history". 
    type: "real" or "virtual"
    """
    symbol = symbol.upper()
    trades = []
    market_type = "real_market" if type == "real" else type

    if mode == "history":
        # --- LẤY TỪ CLICKHOUSE (Cold Data) ---
        try:
            # [UPDATED] Query theo đúng schema bạn cung cấp:
            query = f"""
                SELECT TradeTime, Price, Quantity, IsBuyerMaker 
                FROM trades 
                WHERE Symbol = '{symbol}'
            """
            
            # Lưu ý: Bảng trades hiện tại không có cột BuyerID/SellerID nên không filter user_id ở đây
            
            query += f" ORDER BY TradeTime DESC LIMIT {limit or 100}"
            
            rows = ch_client.execute(query)
            
            for row in rows:
                ts = row[0]
                if isinstance(ts, datetime):
                    ts = ts.strftime("%H:%M:%S")
                
                trades.append({
                    "time": ts,
                    "price": float(row[1]),
                    "amount": float(row[2]),
                    "is_buyer_maker": row[3] # True/False
                })
        except Exception as e:
            print(f"⚠️ ClickHouse Error: {e}")

    elif mode == "real_time":
        # --- LẤY TỪ REDIS (Hot Data) ---
        if not limit: limit = 30
        
        try:
            # Xác định Key Redis
            if user_id:
                # Key riêng cho user (Nếu hệ thống matching engine có ghi)
                redis_key = f"user:{user_id}:trades:{symbol}"
            else:
                # Key chung thị trường (Dạng List)
                redis_key = f"trades_50:{market_type}:{symbol}"

            # Lấy danh sách (Redis List)
            raw_list = redis_client.lrange(redis_key, 0, limit - 1)
            
            for item_str in raw_list:
                try:
                    d = json.loads(item_str)
                    
                    # Xử lý thời gian
                    # Redis JSON thường có key: TradeTime, Price, Quantity... (PascalCase)
                    raw_t = d.get("TradeTime", "") or d.get("time", "")
                    ts = raw_t.split(" ")[1].split(".")[0] if " " in raw_t else raw_t
                    
                    trades.append({
                        "time": ts,
                        "price": float(d.get("Price") or d.get("price", 0)),
                        "amount": float(d.get("Quantity") or d.get("quantity", 0)),
                        "is_buyer_maker": d.get("IsBuyerMaker") or d.get("is_buyer_maker", False)
                    })
                except: 
                    continue
                    
        except Exception as e:
            print(f"⚠️ Redis Error: {e}")

    return trades