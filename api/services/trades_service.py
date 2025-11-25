"""
Service layer cho trade data và price history
"""

from typing import Dict, Any, List
from db import ch_client, redis_client
from config import INTERVAL_MAP
import json

def get_trades(symbol: str, mode="real_time", type="real", user_id=None, limit=None):
    """
    Dùng để lấy lịch sử giao dịch khớp.
    mode: "real_time" or "history". 
    type: "real" or "virtual"
    """
    symbol = symbol.upper()
    trades = []
    if (mode == "history"):
        """
        Hàm này dùng để lấy toàn bộ dữ liệu từ clickhouse
        """
    
    elif (mode == "real_time"):
        """
        Hàm này dùng để lấy những giao dịch mới nhất 
        """
        if not limit: limit = 30

        try:
            raw = redis_client.get(f"trades:{type}:{symbol}")
            # TODO: CHECK cho mỗi user thì sao
            if user_id: raw = redis_client.get(f"user:{user_id}:trades:{symbol}")
            # TODO: SỬa đoạn này nữa
            if raw:
                d = json.loads(raw)
                raw_t = d.get("TradeTime", "")
                ts = raw_t.split(" ")[1].split(".")[0] if " " in raw_t else raw_t
                trades.append({"time": ts, "price": float(d["Price"]), "amount": float(d["Quantity"]), "is_buyer_maker": d["IsBuyerMaker"]})
        except: pass
    return trades
