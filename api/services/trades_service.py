from db import ch_client, redis_client
import json
from datetime import datetime

def get_trades(symbol: str, mode: str = "real_time", type: str = "real", limit: int = 50):
    """
    Lấy lịch sử giao dịch (Public Market Trades).
    - mode: "real_time" (Lấy từ Redis) hoặc "history" (Lấy từ ClickHouse)
    - type: "real" (Dữ liệu thật) hoặc "virtual" (Dữ liệu giả lập)
    """
    symbol = symbol.upper()
    trades = []

    # --- MODE: HISTORY (Lấy từ ClickHouse - Cold Data) ---
    if mode == "history":
        try:
            # Giả sử ClickHouse lưu chung bảng trades, có cột Type hoặc tách bảng
            # Ở đây query demo cho bảng trades cơ bản
            query = f"""
                SELECT TradeTime, Price, Quantity, IsBuyerMaker 
                FROM trades 
                WHERE Symbol = '{symbol}'
                AND Type = '{type}'
                ORDER BY TradeTime DESC LIMIT {limit}
            """
            rows = ch_client.execute(query)
            for row in rows:
                # Xử lý datetime từ ClickHouse
                ts = row[0]
                time_str = ts.strftime("%H:%M:%S") if isinstance(ts, datetime) else str(ts)
                
                trades.append({
                    "time": time_str,
                    "price": float(row[1]),
                    "amount": float(row[2]),
                    "side": "sell" if row[3] else "buy" # IsBuyerMaker=True -> Sell (Taker là Sell)
                })
        except Exception as e:
            print(f"⚠️ ClickHouse Error: {e}")
            return []

    # --- MODE: REAL TIME (Lấy từ Redis - Hot Data) ---
    elif mode == "real_time":
        try:
            if type == "real":
                # --- LOGIC CHO REAL TRADES (List JSON) ---
                # Key: trades:real:BTCUSDT
                key = f"trades:real:{symbol}"
                raw_list = redis_client.lrange(key, 0, limit - 1)
                
                for item in raw_list:
                    try:
                        d = json.loads(item)
                        # Schema JSON: {"Symbol": "BNBUSDT", "TradeID": 1329490349, "Price": 856.25, "Quantity": 0.087, "EventTime": "2025-11-26 00:11:24.763000",
                        #  "TradeTime": "2025-11-26 00:11:24.763000", "IsBuyerMaker": false,
                        #  "Side": "BUY", "TradeValue": 74.49374999999999, "Type": "Real", "Year": 2025, "Month": 11, "Day": 26, "Hour": 0}
                        
                        # Xử lý time: Có thể là int timestamp hoặc string
                        raw_t = d.get('TradeTime')
                        if isinstance(raw_t, (int, float)):
                            time_str = datetime.fromtimestamp(raw_t).strftime("%H:%M:%S")
                        else:
                            # Nếu là string "2024-..." cắt lấy giờ
                            time_str = str(raw_t).split(" ")[1].split(".")[0] if " " in str(raw_t) else str(raw_t)

                        trades.append({
                            "price": float(d.get('Price', 0)),
                            "amount": float(d.get('Quantity', 0)),
                            "side": d.get('Side'), 
                            "time": time_str
                        })
                    except: continue

            else: 
                # --- LOGIC CHO VIRTUAL TRADES (List ID -> Hash Detail) ---
                # Key List: trades:virtual:BTCUSDT
                key = f"trades:virtual:{symbol}" 
                trade_ids = redis_client.lrange(key, 0, limit - 1)
                
                if not trade_ids:
                    return []

                # Dùng Pipeline lấy chi tiết từng trade
                pipe = redis_client.pipeline()
                for tid in trade_ids:
                    pipe.hgetall(f"trade:virtual:{tid}")
                results = pipe.execute()
                
                for r in results:
                    if r and "price" in r:
                        # Schema Hash: {price, amount, taker_side, timestamp, ...}
                        try:
                            ts = float(r.get('timestamp', 0))
                            time_str = datetime.fromtimestamp(ts).strftime("%H:%M:%S")
                            
                            trades.append({
                                "price": float(r.get('price')),
                                "amount": float(r.get('amount')),
                                "side": r.get('taker_side'), # 'buy' hoặc 'sell'
                                "time": time_str
                            })
                        except: continue

        except Exception as e:
            print(f"⚠️ Redis Error: {e}")
            return []

    return trades

def get_user_trade_history(user_id: str, limit: int = 50):
    """
    Lấy lịch sử giao dịch của riêng User (My Trades).
    Data này được Engine push vào List riêng của user khi khớp lệnh.
    """
    # Key: user:{id}:trades (List chứa JSON snapshot)
    key = f"user:{user_id}:trades"
    
    try:
        raw_list = redis_client.lrange(key, 0, limit - 1)
        history = []
        for item in raw_list:
            try:
                history.append(json.loads(item))
            except: continue
        return history
    except Exception as e:
        print(f"⚠️ Redis Error (User History): {e}")
        return []