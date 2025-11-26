from db import redis_client, ch_client
import json
from typing import List

# Lấy toàn bộ mã 
def get_all_symbols() -> List[str]:
    """
    Lấy danh sách symbols có data trong database
    
    Returns:
        List các symbol strings
    """
    client = ch_client
    
    query = """
    SELECT DISTINCT Symbol 
    FROM trades
    ORDER BY Symbol
    """
    
    result = client.execute(query)
    
    return [row[0] for row in result]

# Lấy toàn bộ tickers của các mã 
def get_tickers():
    symbols = get_all_symbols()
    results = []
    for symbol in symbols:
        try:
            raw = redis_client.get(f"ticker_1d:{symbol}")
            if raw:
                d = json.loads(raw)
                open_p = float(d.get("Open_price", 1))
                close_p = float(d.get("Close_price", 0))
                change = ((close_p - open_p) / open_p * 100) if open_p != 0 else 0
                
                results.append({
                    "symbol": symbol,
                    "price": close_p,
                    "open": open_p,
                    "close": close_p,
                    "change": round(change, 2),
                    "high": float(d.get("High_price", 0)),
                    "low": float(d.get("Low_price", 0)),
                    "volume": float(d.get("Volume", 0))
                })
        except: continue
    return results
