import redis
import json
import time
import datetime
from config.setting import REDIS_HOST, REDIS_PORT

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
    Ghi dữ liệu Orderbook:
    - Score = Timestamp (Để lấy 50 cái mới nhất).
    - Value = Price.
    - Logic khớp lệnh (Engine) sẽ phải lôi data về và tự sort lại theo Giá.
    """
    def process_partition(iterator):
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        pipe = r.pipeline()
        
        count = 0
        for row in iterator:
            data = row.asDict()
            symbol = data.get("Symbol")
            if not symbol: continue

            # Lấy thời gian từ sự kiện hoặc dùng thời gian hiện tại của server
            # Format mẫu: "2025-11-25 00:05:05.314000"
            event_time_str = data.get("Event_time")
            try:
                # Chuyển đổi string time sang timestamp (float)
                # Nếu chuỗi thời gian có định dạng cố định, dùng strptime
                dt_obj = datetime.strptime(event_time_str, "%Y-%m-%d %H:%M:%S.%f")
                timestamp = dt_obj.timestamp()
            except:
                # Fallback nếu lỗi format
                timestamp = time.time()

            # --- 1. XỬ LÝ ASKS (BÁN) ---
            ask_z_key = f"orderbook:{mode}:{symbol}:asks"
            ask_h_key = f"orderbook:{mode}:{symbol}:asks:vol"
            
            ask_prices = data.get("Ask_prices", [])
            ask_quantities = data.get("Ask_quantities", [])

            for p, q in zip(ask_prices, ask_quantities):
                price_str = str(p)
                vol_float = float(q)

                if vol_float > 0:
                    # QUAN TRỌNG: Score bây giờ là TIMESTAMP
                    # Lệnh nào mới cập nhật sẽ có timestamp lớn hơn -> nằm ở cuối ZSET
                    pipe.zadd(ask_z_key, {price_str: timestamp})
                    pipe.hset(ask_h_key, price_str, vol_float)
                else:
                    # Volume = 0 -> Xóa
                    pipe.zrem(ask_z_key, price_str)
                    pipe.hdel(ask_h_key, price_str)
            
            # TRIM: GIỮ 50 CÁI MỚI NHẤT
            # ZSET sắp xếp theo Time tăng dần: [Cũ nhất, ..., Mới nhất]
            # Muốn giữ 50 cái Mới nhất (Cuối cùng), ta xóa từ 0 đến -51
            pipe.zremrangebyrank(ask_z_key, 0, -51)


            # --- 2. XỬ LÝ BIDS (MUA) ---
            bid_z_key = f"orderbook:{mode}:{symbol}:bids"
            bid_h_key = f"orderbook:{mode}:{symbol}:bids:vol"
            
            bid_prices = data.get("Bid_prices", [])
            bid_quantities = data.get("Bid_quantities", [])

            for p, q in zip(bid_prices, bid_quantities):
                price_str = str(p)
                vol_float = float(q)

                if vol_float > 0:
                    # Score là TIMESTAMP
                    pipe.zadd(bid_z_key, {price_str: timestamp})
                    pipe.hset(bid_h_key, price_str, vol_float)
                else:
                    pipe.zrem(bid_z_key, price_str)
                    pipe.hdel(bid_h_key, price_str)

            # TRIM: GIỮ 50 CÁI MỚI NHẤT
            # Logic y hệt Asks vì ta đang sort theo Thời gian, không phải Giá
            pipe.zremrangebyrank(bid_z_key, 0, -51)
            
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