from db import r, ch_client
import json
from datetime import datetime

# [UPDATE] Thêm tham số limit
def get_kline_hybrid_logic(symbol: str, interval: str, limit: int = 500):
    symbol = symbol.upper()
    
    # 1. ClickHouse (History)
    history_data = []
    try:
        # Query động với limit được truyền vào
        query = f"""
            SELECT Open_time, Open, High, Low, Close, Volume 
            FROM klines 
            WHERE Symbol = '{symbol}' AND Interval = '{interval}'
            ORDER BY Open_time DESC, Event_time DESC
            LIMIT 1 BY Open_time
            LIMIT {limit} 
        """
        rows = ch_client.execute(query)
        for row in rows:
            ts = str(row[0]) if isinstance(row[0], datetime) else row[0]
            history_data.append({
                "timestamp": ts,
                "open": float(row[1]), "high": float(row[2]),
                "low": float(row[3]), "close": float(row[4]),
                "volume": float(row[5])
            })
        history_data.reverse()
    except Exception as e:
        print(f"⚠️ ClickHouse Error: {e}")

    # 2. Redis (Realtime)
    redis_key = f"kline:{symbol}:{interval}"
    raw_data = r.get(redis_key)
    if not raw_data: raw_data = r.get(f"kline_{symbol}_{interval}") 

    realtime_candle = None
    if raw_data:
        try:
            d = json.loads(raw_data)
            realtime_candle = {
                "timestamp": d.get("Open_time"), 
                "open": float(d.get("Open", 0)), "high": float(d.get("High", 0)),
                "low": float(d.get("Low", 0)), "close": float(d.get("Close", 0)),
                "volume": float(d.get("Volume", 0))
            }
        except: pass

    # 3. Merge
    final_data = history_data
    if realtime_candle:
        if len(final_data) > 0:
            if str(final_data[-1]['timestamp']) == str(realtime_candle['timestamp']):
                final_data[-1] = realtime_candle
            else:
                final_data.append(realtime_candle)
        else:
            final_data.append(realtime_candle)

    return final_data