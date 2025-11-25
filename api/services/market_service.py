
from db import r, ch_client
import json
from datetime import datetime

def get_tickers_logic():
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "DOGEUSDT"]
    results = []
    for symbol in symbols:
        try:
            raw = r.get(f"ticker:{symbol}")
            if raw:
                d = json.loads(raw)
                open_p = float(d.get("Open_price", 1))
                close_p = float(d.get("Close_price", 0))
                change = ((close_p - open_p) / open_p * 100) if open_p != 0 else 0
                
                results.append({
                    "symbol": symbol,
                    "price": close_p,
                    "change": round(change, 2),
                    "high": float(d.get("High_price", 0)),
                    "low": float(d.get("Low_price", 0)),
                    "volume": float(d.get("Volume", 0))
                })
        except: continue
    return results

# [UPDATE] Thêm tham số limit
def get_recent_trades_logic(symbol: str, limit: int = 20):
    symbol = symbol.upper()
    trades = []
    try:
        # Query với limit động
        query = f"""
            SELECT TradeTime, Price, Quantity, IsBuyerMaker 
            FROM trades 
            WHERE Symbol = '{symbol}' 
            ORDER BY TradeTime DESC 
            LIMIT {limit}
        """
        rows = ch_client.execute(query)
        for row in rows:
            ts = row[0].strftime("%H:%M:%S") if isinstance(row[0], datetime) else str(row[0])
            trades.append({"time": ts, "price": float(row[1]), "amount": float(row[2]), "is_buyer_maker": row[3]})
    except:
        # Fallback Redis (chỉ có 1 bản ghi mới nhất)
        try:
            raw = r.get(f"trade:latest:{symbol}")
            if raw:
                d = json.loads(raw)
                raw_t = d.get("TradeTime", "")
                ts = raw_t.split(" ")[1].split(".")[0] if " " in raw_t else raw_t
                trades.append({"time": ts, "price": float(d["Price"]), "amount": float(d["Quantity"]), "is_buyer_maker": d["IsBuyerMaker"]})
        except: pass
    return trades
