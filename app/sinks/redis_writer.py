import redis
import json
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
                pipe.set(f"price:{mode}:{symbol}", price)
                
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
            if not symbol: continue

            # --- 1. XỬ LÝ ASKS (GIÁ BÁN) ---
            # Ưu tiên giá THẤP (Low to High). Redis ZSET mặc định xếp từ thấp lên cao.
            ask_z_key = f"orderbook:{mode}:{symbol}:asks"
            ask_h_key = f"orderbook:{mode}:{symbol}:asks:vol"
            
            ask_prices = data.get("Ask_prices", [])
            ask_quantities = data.get("Ask_quantities", [])

            # a. Cập nhật dữ liệu mới
            for p, q in zip(ask_prices, ask_quantities):
                if float(q) > 0:
                    pipe.zadd(ask_z_key, {str(p): float(p)}) # Score=Price
                    pipe.hset(ask_h_key, str(p), float(q))
                else:
                    # Nếu volume = 0 thì xóa
                    pipe.zrem(ask_z_key, str(p))
                    pipe.hdel(ask_h_key, str(p))
            
            # b. CẮT GỌT (TRIM): Chỉ giữ 50 giá Thấp nhất (0 đến 49)
            # Xóa từ rank 50 đến vô cùng (rank cuối cùng là -1)
            pipe.zremrangebyrank(ask_z_key, 50, -1)


            # --- 2. XỬ LÝ BIDS (GIÁ MUA) ---
            # Ưu tiên giá CAO (High to Low).
            bid_z_key = f"orderbook:{mode}:{symbol}:bids"
            bid_h_key = f"orderbook:{mode}:{symbol}:bids:vol"
            
            bid_prices = data.get("Bid_prices", [])
            bid_quantities = data.get("Bid_quantities", [])

            # a. Cập nhật dữ liệu mới
            for p, q in zip(bid_prices, bid_quantities):
                if float(q) > 0:
                    pipe.zadd(bid_z_key, {str(p): float(p)})
                    pipe.hset(bid_h_key, str(p), float(q))
                else:
                    pipe.zrem(bid_z_key, str(p))
                    pipe.hdel(bid_h_key, str(p))

            # b. CẮT GỌT (TRIM): Chỉ giữ 50 giá Cao nhất
            # Trong ZSET (Thấp->Cao), giá cao nhất nằm ở cuối.
            # Muốn giữ 50 ông cuối, ta xóa từ 0 đến -51.
            pipe.zremrangebyrank(bid_z_key, 0, -51)

            # (Optional) Clean HASH: Bước này để xóa rác trong Hash map cho sạch 
            # nhưng nếu để đơn giản và nhanh thì có thể bỏ qua, Redis chứa vài key rác không sao.
            
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
                pipe.set(f"ticker:{symbol}", json_data)
            
            count += 1
            if count % 100 == 0: pipe.execute()
            
        pipe.execute()
        r.close()

    batch_df.foreachPartition(process_partition)