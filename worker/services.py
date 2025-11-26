import json
from config import redis_client

def get_orderbook(symbol: str, type: str = "real", side: str = "both", limit: int = 100):
    """
    Lấy Orderbook snapshot để hiển thị.
    """
    symbol = symbol.upper()
    result = {}

    def fetch_side(side_key, reverse_sort):
        book_entries = []
        
        if type == "real":
            # 1. Định nghĩa Key
            # ZSET: Chứa danh sách giá (để sort). Ví dụ: orderbook:real:BTCUSDT:bids
            z_key = f"orderbook:{type}:{symbol}:{side_key}"
            
            # HASH: Chứa dữ liệu JSON (như trong ảnh). Ví dụ: orderbook:real:BTCUSDT:bids:data
            h_key = f"orderbook:{type}:{symbol}:{side_key}:data"
            
            # 2. Lấy danh sách GIÁ từ ZSET (Chỉ lấy Key là Giá)
            # - Bids (Mua): Giá Cao -> Thấp (desc=True)
            # - Asks (Bán): Giá Thấp -> Cao (desc=False)
            prices = redis_client.zrange(z_key, 0, limit - 1, desc=reverse_sort)
            
            if not prices:
                return []

            # 3. Lấy dữ liệu chi tiết từ HASH (Pipeline cho nhanh)
            pipe = redis_client.pipeline()
            for p in prices:
                pipe.hget(h_key, p)
            json_data_list = pipe.execute()
            
            # 4. Parse JSON và format dữ liệu trả về
            for js in json_data_list:
                if js:
                    try:
                        d = json.loads(js)
                        # Format: [Price, Amount]
                        book_entries.append([float(d['p']), float(d['a'])])
                    except: continue

        else: 
            # --- LOGIC VIRTUAL (GIỮ NGUYÊN) ---
            z_key = f"orderbook:{type}:{symbol}:{side_key}"
            
            # Virtual lưu ID trong ZSET, không lưu JSON trong Hash như Real
            items_with_score = redis_client.zrange(z_key, 0, limit - 1, desc=reverse_sort, withscores=True)
            
            if not items_with_score: return []

            pipe = redis_client.pipeline()
            for order_id, price in items_with_score:
                pipe.hget(f"order:virtual:{order_id}", "remaining_amount")
            amounts = pipe.execute()
            
            price_map = {}
            for (order_id, price), amount in zip(items_with_score, amounts):
                if amount:
                    p = float(price)
                    a = float(amount)
                    price_map[p] = price_map.get(p, 0) + a
            
            book_entries = [[p, a] for p, a in price_map.items()]
            book_entries.sort(key=lambda x: x[0], reverse=reverse_sort)
            book_entries = book_entries[:limit]

        return book_entries

    # --- GỌI HÀM LẤY DATA ---
    if side in ["bids", "both"]:
        # Bids: Giá Cao nhất đứng đầu (Reverse=True)
        result["bids"] = fetch_side("bids", True)
    
    if side in ["asks", "both"]:
        # Asks: Giá Thấp nhất đứng đầu (Reverse=False)
        result["asks"] = fetch_side("asks", False)

    # Thêm thông tin phụ trợ
    result["symbol"] = symbol
    result["type"] = type

    return result