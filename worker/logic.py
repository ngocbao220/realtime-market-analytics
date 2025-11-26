import json
import uuid
import logging
import time

from config import redis_client
from services import get_orderbook
from lua_script import LUA_MATCH_P2P, LUA_MATCH_REAL
from config import redis_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s - ENGINE - %(message)s")

# Load Lua Scripts
try:
    p2p_sha = redis_client.script_load(LUA_MATCH_P2P)
    real_sha = redis_client.script_load(LUA_MATCH_REAL)
    logging.info("✅ Lua Scripts Loaded")
except Exception as e:
    logging.error(f"❌ Lua Load Error: {e}")

# Lấy thông tin chi tiết của một order dựa vào order_id
def get_order_details(order_id):
    """Helper lấy thông tin lệnh"""
    data = redis_client.hgetall(f"order:virtual:{order_id}")
    if not data: return None
    # Convert số
    for k in ['price', 'amount', 'remaining_amount']:
        if k in data: data[k] = float(data[k])
    return data

# Lưu thông tin trades
def save_trade_log(pipe, symbol, price, amount, taker_side, buyer_id, seller_id, timestamp):
    """
    Hàm lưu lịch sử giao dịch tập trung (Chỉ lưu vào List JSON, bỏ qua Hash).
    """
    # 1. Ghi vào Lịch sử thị trường (Market Trades)
    # Key: trades:virtual:BTCUSDT
    # Lưu JSON trực tiếp để Frontend hiển thị luôn
    market_trade_data = {
        "price": price,
        "amount": amount,
        "side": taker_side, # 'buy' hoặc 'sell' (phe chủ động)
        "time": timestamp   # timestamp float
    }
    market_key = f"trades:virtual:{symbol}"
    pipe.lpush(market_key, json.dumps(market_trade_data))
    pipe.ltrim(market_key, 0, 99) # Giữ 100 trade mới nhất

    # 2. Ghi vào Lịch sử User (My Trades)
    # A. Ghi cho người MUA
    buyer_rec = {
        "symbol": symbol, 
        "price": price, 
        "amount": amount,
        "side": "buy", 
        "role": "taker" if taker_side == "buy" else "maker",
        "time": timestamp
    }
    # Key: user:1:trades hoặc user:3:trades
    buyer_key = f"user:{buyer_id}:trades"
    pipe.lpush(buyer_key, json.dumps(buyer_rec))
    # QUAN TRỌNG: Phải cắt ngắn list, đặc biệt là với System vì nó trade rất nhiều
    pipe.ltrim(buyer_key, 0, 499) # Giữ 500 trade gần nhất cho mỗi user

    # B. Ghi cho người BÁN
    seller_rec = {
        "symbol": symbol, 
        "price": price, 
        "amount": amount,
        "side": "sell", 
        "role": "taker" if taker_side == "sell" else "maker",
        "time": timestamp
    }
    seller_key = f"user:{seller_id}:trades"
    pipe.lpush(seller_key, json.dumps(seller_rec))
    pipe.ltrim(seller_key, 0, 499)

def process_symbol(symbol):
    v_bids_key = f"orderbook:virtual:{symbol}:bids"
    v_asks_key = f"orderbook:virtual:{symbol}:asks"
    
    # --- PHASE 1: KHỚP P2P (NỘI BỘ) ---
    while True:
        pipe = redis_client.pipeline()
        pipe.zrange(v_bids_key, -1, -1) 
        pipe.zrange(v_asks_key, 0, 0)   
        res = pipe.execute()
        
        best_bid_ids = res[0]
        best_ask_ids = res[1]

        if not best_bid_ids or not best_ask_ids: break 
            
        bid_order = get_order_details(best_bid_ids[0])
        ask_order = get_order_details(best_ask_ids[0])
        
        if not bid_order or not ask_order:
            if not bid_order: redis_client.zrem(v_bids_key, best_bid_ids[0])
            if not ask_order: redis_client.zrem(v_asks_key, best_ask_ids[0])
            continue

        if bid_order['price'] >= ask_order['price']:
            # 1. XÁC ĐỊNH MAKER / TAKER DỰA TRÊN THỜI GIAN
            bid_ts = bid_order.get('timestamp_created', 0)
            ask_ts = ask_order.get('timestamp_created', 0)
            
            # Mặc định taker_side
            taker_side = "buy" 
            match_price = ask_order['price'] # Giá mặc định là giá người bán

            if bid_ts > ask_ts:
                # Lệnh Mua vào sau (Mới hơn) -> Mua là Taker
                taker_side = "buy"
                match_price = ask_order['price'] # Khớp theo giá Maker (người bán đang treo)
            else:
                # Lệnh Bán vào sau (Mới hơn) -> Bán là Taker
                taker_side = "sell"
                match_price = bid_order['price'] # Khớp theo giá Maker (người mua đang treo)

            # (Logic lấy min volume giữ nguyên)
            match_qty = min(bid_order['remaining_amount'], ask_order['remaining_amount'])
            
            logging.info(f"⚡ P2P MATCH: {match_qty} {symbol} @ {match_price} (Taker: {taker_side})")
            
            try:
                # Gọi Lua Script (Tham số không đổi)
                redis_client.evalsha(
                    p2p_sha, 6, 
                    f"order:virtual:{bid_order['order_id']}", f"order:virtual:{ask_order['order_id']}",
                    f"user:{bid_order['user_id']}:balance", f"user:{ask_order['user_id']}:balance",
                    v_bids_key, v_asks_key,
                    match_qty, match_price, bid_order['order_id'], ask_order['order_id']
                )

                # 2. GHI LOG (Truyền đúng taker_side vừa tính được)
                pipe = redis_client.pipeline()
                save_trade_log(
                    pipe=pipe,
                    symbol=symbol,
                    price=match_price,
                    amount=match_qty,
                    taker_side=taker_side, # <--- Đã sửa ở đây
                    buyer_id=bid_order['user_id'],
                    seller_id=ask_order['user_id'],
                    timestamp=time.time()
                )
                pipe.execute()

            except Exception as e:
                logging.error(f"P2P Match Error: {e}")
                break
        else:
            break

    # --- PHASE 2: KHỚP REAL ---
    
    # A. Check User MUA vs Real BÁN
    while True:
        best_bid_ids = redis_client.zrange(v_bids_key, -1, -1)
        if not best_bid_ids: break
        
        bid_order = get_order_details(best_bid_ids[0])
        if not bid_order: 
            redis_client.zrem(v_bids_key, best_bid_ids[0])
            continue

        real_asks = get_orderbook(symbol, type="real", side="asks")
        if not real_asks: break
        
        min_real_price = real_asks.get("asks")[0][0] 

        if bid_order['price'] >= min_real_price:
            match_qty = bid_order['remaining_amount']
            logging.info(f"🌊 REAL MATCH (BUY): {match_qty} {symbol} @ {min_real_price}")
            
            try:
                redis_client.evalsha(
                    real_sha, 3,
                    f"order:virtual:{bid_order['order_id']}",
                    f"user:{bid_order['user_id']}:balance",
                    v_bids_key,
                    match_qty, min_real_price, bid_order['order_id'], 'bids'
                )

                # GHI LOG
                pipe = redis_client.pipeline()
                save_trade_log(
                    pipe=pipe,
                    symbol=symbol,
                    price=min_real_price,
                    amount=match_qty,
                    taker_side="buy",
                    buyer_id=bid_order['user_id'],
                    seller_id="3", # System bán
                    timestamp=time.time()
                )
                pipe.execute()

            except Exception as e:
                logging.error(f"Real Match Buy Error: {e}")
                break
        else:
            break 

    # B. Check User BÁN vs Real MUA
    while True:
        best_ask_ids = redis_client.zrange(v_asks_key, 0, 0)
        if not best_ask_ids: break
        
        ask_order = get_order_details(best_ask_ids[0])
        if not ask_order:
            redis_client.zrem(v_asks_key, best_ask_ids[0])
            continue

        real_bids = get_orderbook(symbol, type="real", side="bids")
        if not real_bids: break
    
        max_real_price = real_bids.get("bids")[0][0]

        if ask_order['price'] <= max_real_price:
            match_qty = ask_order['remaining_amount']
            logging.info(f"🌊 REAL MATCH (SELL): {match_qty} {symbol} @ {max_real_price}")
            
            try:
                redis_client.evalsha(
                    real_sha, 3,
                    f"order:virtual:{ask_order['order_id']}",
                    f"user:{ask_order['user_id']}:balance",
                    v_asks_key,
                    match_qty, max_real_price, ask_order['order_id'], 'asks'
                )

                # GHI LOG
                pipe = redis_client.pipeline()
                save_trade_log(
                    pipe=pipe,
                    symbol=symbol,
                    price=max_real_price,
                    amount=match_qty,
                    taker_side="sell",
                    buyer_id="3", # System mua
                    seller_id=ask_order['user_id'],
                    timestamp=time.time()
                )
                pipe.execute()
                
            except Exception as e:
                logging.error(f"Real Match Sell Error: {e}")
                break
        else:
            break