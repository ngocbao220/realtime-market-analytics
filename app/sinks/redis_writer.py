import redis
import json
import time
import datetime
from config.setting import REDIS_HOST, REDIS_PORT
import pytz

# ==========================================
# 1. HÀM GHI TRADES (Giao dịch khớp lệnh)
# ==========================================
def write_trades_to_redis(batch_df, mode="real_market"):
    """
    Ghi dữ liệu khớp lệnh mới nhất.
    Keys:
      - price:{symbol} -> Giá mới nhất (dùng để update giá hiển thị nhanh)
      - trade:latest:{symbol} -> Chi tiết lệnh vừa khớp (cho list recent trades)
    """
    def process_partition(iterator):
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        pipe = r.pipeline()
        count = 0
        
        for row in iterator:
            data = row.asDict()
            symbol = data.get("Symbol")
            price = data.get("Price")
            
            if symbol:
                # Update giá hiển thị nhanh (nhẹ)
                pipe.set(f"current_price:{mode}:{symbol}", price)
                
                # Update chi tiết lệnh khớp
                json_data = json.dumps(data, default=str)
                
                # Lưu 50 trades mới nhất
                pipe.lpush(f"trades_50:{mode}:{symbol}", json_data)
                pipe.ltrim(f"trades_50:{mode}:{symbol}", 0, 49)

            count += 1
            if count % 100 == 0: pipe.execute()
            
        pipe.execute()
        r.close()

    batch_df.foreachPartition(process_partition)


# ==========================================
# 2. HÀM GHI ORDERBOOK (Sổ lệnh)
# ==========================================
def write_orderbook_to_redis(batch_df, mode='real_market'):
    """
    Lưu Orderbook theo nguyên tắc:
    - Score = Timestamp (Để Redis tự sắp xếp thời gian chuẩn xác 100%).
    - Member = JSON String chứa toàn bộ data.
    - Luôn giữ 50 bản ghi có Timestamp lớn nhất (Mới nhất).
    """
    def process_partition(iterator):
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        pipe = r.pipeline()
        
        count = 0
        for row in iterator:
            data = row.asDict()
            symbol = data.get("Symbol")
            if not symbol: continue

            # --- SỬA LỖI Ở ĐÂY ---
            raw_time = data.get("Event_time")
            
            # 1. Xử lý Time String (để lưu vào JSON cho người đọc)
            if isinstance(raw_time, (datetime.datetime, datetime.time)):
                # Nếu là object datetime -> Chuyển thành chuỗi String đẹp
                readable_time_str = raw_time.strftime("%Y-%m-%d %H:%M:%S.%f")
                # Lấy timestamp số để làm Score sắp xếp
                sort_score = raw_time.timestamp()
            else:
                # Nếu là string hoặc số sẵn rồi
                readable_time_str = str(raw_time)
                sort_score = time.time()

            # --- XỬ LÝ ASKS ---
            ask_key = f"orderbook:{mode}:{symbol}:asks"
            ask_prices = data.get("Ask_prices", [])
            ask_quantities = data.get("Ask_quantities", [])

            for p, q in zip(ask_prices, ask_quantities):
                if float(q) > 0:
                    record = {
                        "t": readable_time_str, 
                        "p": float(p),
                        "a": float(q)
                    }
                    # json.dumps giờ sẽ không lỗi nữa vì "t" là string
                    pipe.zadd(ask_key, {json.dumps(record): sort_score})

            pipe.zremrangebyrank(ask_key, 0, -51)

            # --- XỬ LÝ BIDS ---
            bid_key = f"orderbook:{mode}:{symbol}:bids"
            bid_prices = data.get("Bid_prices", [])
            bid_quantities = data.get("Bid_quantities", [])

            for p, q in zip(bid_prices, bid_quantities):
                if float(q) > 0:
                    record = {
                        "t": readable_time_str,
                        "p": float(p),
                        "a": float(q)
                    }
                    pipe.zadd(bid_key, {json.dumps(record): sort_score})

            pipe.zremrangebyrank(bid_key, 0, -51)
            
            count += 1
            if count % 20 == 0: pipe.execute()
            
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
        count = 0
        
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
            
            count += 1
            if count % 100 == 0: pipe.execute()
            
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
        count = 0
        
        for row in iterator:
            data = row.asDict()
            symbol = data.get("Symbol")
            
            if symbol:
                json_data = json.dumps(data, default=str)
                pipe.set(f"ticker_1d:{symbol}", json_data)
            
            count += 1
            if count % 100 == 0: pipe.execute()
            
        pipe.execute()
        r.close()

    batch_df.foreachPartition(process_partition)