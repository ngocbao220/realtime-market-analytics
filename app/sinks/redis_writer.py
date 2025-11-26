import redis
import json
import time
import datetime
from config.setting import REDIS_HOST, REDIS_PORT, SPEED, ORDERBOOK_DEPTH

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

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
# 2. HÀM GHI ORDERBOOK (Sổ lệnh)
# ==========================================
def write_orderbook_to_redis(batch_df, type='real'):
    """
    Logic chuẩn: MERGE -> SORT -> TRIM
    1. Merge: Cập nhật toàn bộ data mới vào Redis (không cắt input).
    2. Sort: Redis ZSET tự động sắp xếp.
    3. Trim: Chỉ giữ lại 100 lệnh tốt nhất trong Redis, xóa phần thừa ở cả ZSET và HASH.

    Kết quả như sau:
    - Đối với asks thì là 100 thằng có giá mua cao nhất
    - Đối với bids thì là 100 thằng có giá bán thấp nhất
    => Cả 2 thằng đều được sắp xếp từ thấp đến cao nên lúc lấy từ redis
    để so lệnh khớp thì nhớ lấy thằng cuối cùng của bids để so s
    """
    def process_partition(iterator):
        pipe = r.pipeline()
        
        for row in iterator:
            data = row.asDict()
            symbol = data.get("Symbol")
            if not symbol: continue

            # Xử lý thời gian
            raw_time = data.get("Event_time")
            if isinstance(raw_time, (datetime.datetime, datetime.time)):
                readable_time_str = raw_time.strftime("%Y-%m-%d %H:%M:%S.%f")
            else:
                readable_time_str = str(raw_time)

            # =========================================================
            # 1. XỬ LÝ ASKS (BÁN) - TỪ THẤP ĐẾN CAO
            # =========================================================

            # Key để lưu lên redis
            ask_z_key = f"orderbook:{type}:{symbol}:asks" 
            ask_h_key = f"orderbook:{type}:{symbol}:asks:data"
            
            # Lấy dữ liệu từ kafka chuyển lên
            ask_prices = data.get("Ask_prices", [])
            ask_quantities = data.get("Ask_quantities", [])

            # BƯỚC 1: MERGE (Cập nhật toàn bộ Input vào Redis)
            for p, q in zip(ask_prices, ask_quantities):
                price_str = str(p)
                vol_float = float(q)

                if vol_float > 0:
                    record = {"t": readable_time_str, "p": float(p), "a": vol_float}
                    pipe.zadd(ask_z_key, {price_str: float(p)}) # Đoạn này đã sắp xếp từ thấp đến cao
                    pipe.hset(ask_h_key, price_str, json.dumps(record))
                else:
                    # Nếu volume = 0 (lệnh hủy/khớp hết) -> Xóa ngay
                    pipe.zrem(ask_z_key, price_str)
                    pipe.hdel(ask_h_key, price_str)

            # Thực thi việc Update trước để Redis có dữ liệu mới nhất
            pipe.execute() 
            
            # BƯỚC 2 & 3: SORT & TRIM (Dọn dẹp rác sau khi đã Merge)
            # Asks trong Redis xếp: [Rẻ nhất (0) ... Đắt nhất (-1)]
            # Ta muốn giữ 0 -> 99. Xóa từ 100 -> Hết.
            cleanup_pipe = r.pipeline()
            
            # Tìm danh sách thừa (nằm ngoài top 100)
            excess_asks = r.zrange(ask_z_key, ORDERBOOK_DEPTH, -1)
            
            if excess_asks:
                cleanup_pipe.zrem(ask_z_key, *excess_asks)     # Xóa trong Index
                cleanup_pipe.hdel(ask_h_key, *excess_asks)     # Xóa trong Data Hash (Quan trọng!)


            # =========================================================
            # 2. XỬ LÝ BIDS (MUA) - TỪ CAO XUỐNG THẤP
            # =========================================================
            bid_z_key = f"orderbook:{type}:{symbol}:bids"
            bid_h_key = f"orderbook:{type}:{symbol}:bids:data"
            
            bid_prices = data.get("Bid_prices", [])
            bid_quantities = data.get("Bid_quantities", [])

            # BƯỚC 1: MERGE
            for p, q in zip(bid_prices, bid_quantities):
                price_str = str(p)
                vol_float = float(q)

                if vol_float > 0:
                    record = {"t": readable_time_str, "p": float(p), "a": vol_float}
                    pipe.zadd(bid_z_key, {price_str: float(p)})
                    pipe.hset(bid_h_key, price_str, json.dumps(record))
                else:
                    pipe.zrem(bid_z_key, price_str)
                    pipe.hdel(bid_h_key, price_str)
            
            # Update trước
            pipe.execute()

            # BƯỚC 2 & 3: SORT & TRIM
            # Bids trong Redis xếp: [Thấp nhất (0) ... Cao nhất (-1)]
            # Giá tốt nhất nằm ở CUỐI. Ta muốn giữ 100 ông cuối.
            # Xóa từ đầu (0) đến -(100 + 1)
            
            # Tìm danh sách thừa (Giá thấp quá mức)
            excess_bids = r.zrange(bid_z_key, 0, -(ORDERBOOK_DEPTH + 1))
            
            if excess_bids:
                cleanup_pipe.zrem(bid_z_key, *excess_bids)
                cleanup_pipe.hdel(bid_h_key, *excess_bids)

            # Thực thi lệnh dọn dẹp
            cleanup_pipe.execute()
            
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