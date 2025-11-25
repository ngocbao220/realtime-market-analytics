from db import redis_client
import json

def get_orderbook(symbol: str, type: str = "real", side: str = "both", limit: int = 20):
    """
    Lấy Orderbook snapshot.
    - type='real': Parse JSON từ ZSET (sort time) -> Aggregate -> Sort Price.
    - type='virtual': Lấy OrderID từ ZSET -> Lấy chi tiết -> Aggregate -> Sort Price.
    """
    symbol = symbol.upper()
    result = {}

    def fetch_side(side_key, reverse_sort):
        book_entries = []
        
        # --- TRƯỜNG HỢP 1: DỮ LIỆU THẬT (REAL) ---
        if type == "real":
            # Key: orderbook:real:BTCUSDT:bids
            z_key = f"orderbook:{type}:{symbol}:{side_key}"
            
            # 1. Lấy toàn bộ 50 bản ghi mới nhất (đang xếp theo thời gian)
            # Vì số lượng ít (50) nên lấy hết về Python xử lý rất nhanh
            items = redis_client.zrange(z_key, 0, -1)
            
            # 2. Parse JSON và Cộng dồn Volume theo mức giá (Aggregate)
            price_map = {}
            for item_str in items:
                try:
                    d = json.loads(item_str)
                    p = float(d.get("p", 0))
                    a = float(d.get("a", 0))
                    
                    if a > 0:
                        # Cộng dồn volume nếu cùng mức giá
                        price_map[p] = price_map.get(p, 0) + a
                except:
                    continue
            
            # 3. Chuyển thành list [[price, vol], ...]
            book_entries = [[p, v] for p, v in price_map.items()]

        # --- TRƯỜNG HỢP 2: DỮ LIỆU GIẢ LẬP (VIRTUAL) ---
        else:
            # Key: orderbook:virtual:BTCUSDT:bids
            z_key = f"orderbook:{type}:{symbol}:{side_key}"
            
            # 1. Lấy Order ID (Virtual lưu Score=Price, Member=OrderID)
            # Lấy dư ra một chút (limit * 2) để trừ hao việc gộp lệnh
            items_with_score = redis_client.zrange(z_key, 0, -1, withscores=True)
            
            if not items_with_score:
                return []

            # 2. Pipeline lấy chi tiết remaining_amount từ Order Hash
            pipe = redis_client.pipeline()
            for order_id, price in items_with_score:
                pipe.hget(f"order:virtual:{order_id}", "remaining_amount")
            amounts = pipe.execute()
            
            # 3. Aggregate
            price_map = {}
            for (order_id, price), amount in zip(items_with_score, amounts):
                if amount:
                    p = float(price)
                    a = float(amount)
                    price_map[p] = price_map.get(p, 0) + a
            
            book_entries = [[p, v] for p, v in price_map.items()]

        # --- BƯỚC CUỐI: SẮP XẾP THEO GIÁ & CẮT LIMIT ---
        # Bids (Mua): Giá cao nhất lên đầu -> reverse=True
        # Asks (Bán): Giá thấp nhất lên đầu -> reverse=False
        book_entries.sort(key=lambda x: x[0], reverse=reverse_sort)
        
        return book_entries[:limit]

    # --- MAIN FLOW ---
    if side in ["bids", "both"]:
        # Bids: Sắp xếp giảm dần (Cao -> Thấp)
        result["bids"] = fetch_side("bids", reverse_sort=True)
    
    if side in ["asks", "both"]:
        # Asks: Sắp xếp tăng dần (Thấp -> Cao)
        result["asks"] = fetch_side("asks", reverse_sort=False)

    return result