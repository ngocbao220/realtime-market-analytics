import redis
import json
import time
import datetime
from config.setting import REDIS_HOST, REDIS_PORT, SPEED, ORDERBOOK_DEPTH

# ==========================================
# 1. HÀM GHI TRADES (Giao dịch khớp lệnh)
# ==========================================
def write_trades_to_redis(batch_df, type="real"):
    """
    Ghi dữ liệu khớp lệnh mới nhất.
    Keys:
      - price:{symbol} -> Giá mới nhất (dùng để update giá hiển thị nhanh)
      - trade:latest:{symbol} -> Chi tiết lệnh vừa khớp (cho list recent trades)
    """
    def process_partition(iterator):
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        pipe = r.pipeline()

        last_exec_time = time.time()
        for row in iterator:
            data = row.asDict()
            symbol = data.get("Symbol")
            price = data.get("Price")
            
            if symbol:
                # Update giá hiển thị nhanh (nhẹ)
                pipe.set(f"current_price:{type}:{symbol}", price)
                
                # Update chi tiết lệnh khớp
                json_data = json.dumps(data, default=str)
                
                pipe.lpush(f"trades:{type}:{symbol}", json_data)
                pipe.ltrim(f"trades:{type}:{symbol}", 0, 99)

            if time.time() - last_exec_time >= SPEED:
                pipe.execute()
                last_exec_time = time.time()
            
        pipe.execute()
        r.close()

    batch_df.foreachPartition(process_partition)

# ==========================================
# 2. HÀM GHI ORDERBOOK (Sổ lệnh) - CỐ ĐỊNH ĐỘ SÂU
# ==========================================
def write_orderbook_to_redis(batch_df, type='real'):
    def process_partition(iterator):
        pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r = redis.Redis(connection_pool=pool)
        pipe = r.pipeline(transaction=False)
        
        for row in iterator:
            data = row.asDict()
            symbol = data.get("Symbol")
            if not symbol: 
                continue
            
            # Timestamp
            raw_time = data.get("Event_time")
            ts = raw_time.strftime("%Y-%m-%d %H:%M:%S.%f") if raw_time else str(datetime.datetime.now())
            
            # =========================================================
            # XỬ LÝ ASKS (BÁN) - ĐẢM BẢO CỐ ĐỊNH ORDERBOOK_DEPTH
            # =========================================================
            ask_prices = data.get("Ask_prices", [])
            ask_qtys = data.get("Ask_quantities", [])
            
            if ask_prices and len(ask_prices) > 0:
                ask_z = f"orderbook:real:{symbol}:asks"
                ask_h = f"orderbook:real:{symbol}:asks:data"
                
                # Lấy dữ liệu cũ từ Redis để bổ sung nếu thiếu
                old_asks = []
                if len(ask_prices) < ORDERBOOK_DEPTH:
                    try:
                        old_ask_prices = r.zrange(ask_z, 0, -1, withscores=True)
                        for p_str, score in old_ask_prices:
                            old_data = r.hget(ask_h, p_str)
                            if old_data:
                                old_asks.append(json.loads(old_data))
                    except:
                        pass
                
                # Xóa dữ liệu cũ
                pipe.delete(ask_z)
                pipe.delete(ask_h)
                
                # Ghi dữ liệu mới
                count = 0
                for p, q in zip(ask_prices, ask_qtys):
                    if count >= ORDERBOOK_DEPTH:
                        break
                    q_float = float(q)
                    if q_float > 0:
                        p_str = str(p)
                        pipe.zadd(ask_z, {p_str: float(p)})
                        pipe.hset(ask_h, p_str, json.dumps({"t": ts, "p": float(p), "a": q_float}))
                        count += 1
                
                # Bổ sung từ dữ liệu cũ nếu chưa đủ ORDERBOOK_DEPTH
                if count < ORDERBOOK_DEPTH and old_asks:
                    # Sắp xếp asks cũ theo giá tăng dần
                    old_asks_sorted = sorted(old_asks, key=lambda x: x['p'])
                    # Lọc bỏ các giá đã có trong dữ liệu mới
                    new_prices_set = set(ask_prices)
                    for old_ask in old_asks_sorted:
                        if count >= ORDERBOOK_DEPTH:
                            break
                        if old_ask['p'] not in new_prices_set:
                            p_str = str(old_ask['p'])
                            pipe.zadd(ask_z, {p_str: old_ask['p']})
                            pipe.hset(ask_h, p_str, json.dumps(old_ask))
                            count += 1
            
            # =========================================================
            # XỬ LÝ BIDS (MUA) - ĐẢM BẢO CỐ ĐỊNH ORDERBOOK_DEPTH
            # =========================================================
            bid_prices = data.get("Bid_prices", [])
            bid_qtys = data.get("Bid_quantities", [])
            
            if bid_prices and len(bid_prices) > 0:
                bid_z = f"orderbook:real:{symbol}:bids"
                bid_h = f"orderbook:real:{symbol}:bids:data"
                
                # Lấy dữ liệu cũ từ Redis để bổ sung nếu thiếu
                old_bids = []
                if len(bid_prices) < ORDERBOOK_DEPTH:
                    try:
                        old_bid_prices = r.zrevrange(bid_z, 0, -1, withscores=True)
                        for p_str, score in old_bid_prices:
                            old_data = r.hget(bid_h, p_str)
                            if old_data:
                                old_bids.append(json.loads(old_data))
                    except:
                        pass
                
                # Xóa dữ liệu cũ
                pipe.delete(bid_z)
                pipe.delete(bid_h)
                
                # Ghi dữ liệu mới
                count = 0
                for p, q in zip(bid_prices, bid_qtys):
                    if count >= ORDERBOOK_DEPTH:
                        break
                    q_float = float(q)
                    if q_float > 0:
                        p_str = str(p)
                        pipe.zadd(bid_z, {p_str: float(p)})
                        pipe.hset(bid_h, p_str, json.dumps({"t": ts, "p": float(p), "a": q_float}))
                        count += 1
                
                # Bổ sung từ dữ liệu cũ nếu chưa đủ ORDERBOOK_DEPTH
                if count < ORDERBOOK_DEPTH and old_bids:
                    # Sắp xếp bids cũ theo giá giảm dần
                    old_bids_sorted = sorted(old_bids, key=lambda x: x['p'], reverse=True)
                    # Lọc bỏ các giá đã có trong dữ liệu mới
                    new_prices_set = set(bid_prices)
                    for old_bid in old_bids_sorted:
                        if count >= ORDERBOOK_DEPTH:
                            break
                        if old_bid['p'] not in new_prices_set:
                            p_str = str(old_bid['p'])
                            pipe.zadd(bid_z, {p_str: old_bid['p']})
                            pipe.hset(bid_h, p_str, json.dumps(old_bid))
                            count += 1
            
            # Thực thi pipeline
            pipe.execute()
        
        r.close()
    
    batch_df.foreachPartition(process_partition)

# ==========================================
# 3. HÀM GHI KLINE (Nến - OHLCV)
# ==========================================
def write_kline_to_redis(batch_df):
    """
    Ghi dữ liệu Nến.
    """
    def process_partition(iterator):
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        pipe = r.pipeline()
        last_exec_time = time.time()
        
        for row in iterator:
            data = row.asDict()
            symbol = data.get("Symbol")
            interval = data.get("Interval", "1m") # Mặc định 1m nếu không có cột Interval
            
            if symbol:
                # Key ví dụ: kline:BTCUSDT:1m
                redis_key = f"kline:{symbol}:{interval}"
                json_data = json.dumps(data, default=str)
                
                # Ghi đè nến hiện tại
                pipe.set(redis_key, json_data)

            if time.time() - last_exec_time >= SPEED:
                pipe.execute()
                last_exec_time = time.time()
            
        pipe.execute()
        r.close()

    batch_df.foreachPartition(process_partition)


# ==========================================
# 4. HÀM GHI TICKER (Thống kê 24h)
# ==========================================
def write_ticker_to_redis(batch_df):
    """
    Ghi dữ liệu thống kê thị trường (24h change, High, Low, Volume).
    Giả định DataFrame có: Symbol, PriceChangePercent, HighPrice, LowPrice, QuoteVolume
    Key: ticker:{symbol}
    """
    def process_partition(iterator):
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        pipe = r.pipeline()
        last_exec_time = time.time()
        
        for row in iterator:
            data = row.asDict()
            symbol = data.get("Symbol")
            
            if symbol:
                json_data = json.dumps(data, default=str)
                pipe.set(f"ticker_1d:{symbol}", json_data)
            
            if time.time() - last_exec_time >= SPEED:
                pipe.execute()
                last_exec_time = time.time()
            
        pipe.execute()
        r.close()

    batch_df.foreachPartition(process_partition)