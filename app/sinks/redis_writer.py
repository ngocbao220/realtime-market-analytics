import redis
import json
from config.setting import REDIS_HOST, REDIS_PORT

# ==========================================
# 1. HÀM GHI TRADES (Giao dịch khớp lệnh)
# ==========================================
def write_trades_to_redis(batch_df):
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
                pipe.set(f"price:{symbol}", price)
                
                # Update chi tiết lệnh khớp
                json_data = json.dumps(data, default=str)
                pipe.set(f"trade:latest:{symbol}", json_data)
                
                # (Optional) Nếu muốn lưu lịch sử 50 trade gần nhất:
                # pipe.lpush(f"trades:list:{symbol}", json_data)
                # pipe.ltrim(f"trades:list:{symbol}", 0, 49)

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
    Ghi dữ liệu Orderbook (Bids/Asks).
    Giả định DataFrame có cột: Symbol, Bids (array/json), Asks (array/json), UpdateTime
    Key: orderbook:{symbol}
    """
    def process_partition(iterator):
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        pipe = r.pipeline()
        count = 0
        
        for row in iterator:
            data = row.asDict()
            symbol = data.get("Symbol")
            
            if symbol:
                # Orderbook thường nặng, chỉ lưu JSON 1 cục
                json_data = json.dumps(data, default=str)
                pipe.set(f"orderbook:{mode}:{symbol}", json_data)
            
            count += 1
            if count % 50 == 0: pipe.execute() # Batch nhỏ hơn vì data orderbook lớn
            
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
                pipe.set(f"ticker:{symbol}", json_data)
            
            count += 1
            if count % 100 == 0: pipe.execute()
            
        pipe.execute()
        r.close()

    batch_df.foreachPartition(process_partition)