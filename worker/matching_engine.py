import logging
import time

from config import redis_client
from helper import get_order_details, save_order_history, save_trade_log
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

# Xử lý khớp lệnh chính
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
            # XÁC ĐỊNH TAKER
            bid_ts = bid_order.get('timestamp_created', 0)
            ask_ts = ask_order.get('timestamp_created', 0)
            
            taker_side = "buy"
            match_price = ask_order['price']

            if bid_ts > ask_ts:
                taker_side = "buy"
                match_price = ask_order['price']
            else:
                taker_side = "sell"
                match_price = bid_order['price']

            match_qty = min(bid_order['remaining_amount'], ask_order['remaining_amount'])
            
            logging.info(f"⚡ P2P MATCH: {match_qty} {symbol} @ {match_price} (Taker: {taker_side})")
            
            try:
                # 1. Gọi Lua Script (Trả về số dư còn lại của lệnh)
                res_lua = redis_client.evalsha(
                    p2p_sha, 6, 
                    f"order:virtual:{bid_order['order_id']}", f"order:virtual:{ask_order['order_id']}",
                    f"user:{bid_order['user_id']}:balance", f"user:{ask_order['user_id']}:balance",
                    v_bids_key, v_asks_key,
                    match_qty, match_price, bid_order['order_id'], ask_order['order_id']
                )
                # Lua trả về [buy_remaining, sell_remaining]
                buy_rem, sell_rem = float(res_lua[0]), float(res_lua[1])

                timestamp = time.time()
                pipe = redis_client.pipeline()

                # 2. GHI TRADE HISTORY (Khớp lệnh)
                save_trade_log(
                    pipe=pipe, symbol=symbol, price=match_price, amount=match_qty,
                    taker_side=taker_side, buyer_id=bid_order['user_id'], seller_id=ask_order['user_id'],
                    timestamp=timestamp
                )

                # 3. GHI ORDER HISTORY (Cập nhật trạng thái lệnh)
                # A. Cho người Mua
                buy_status = "FILLED" if buy_rem <= 1e-8 else "PARTIALLY_FILLED"
                save_order_history(
                    pipe=pipe, order_id=bid_order['order_id'], user_id=bid_order['user_id'],
                    symbol=symbol, side="buy", price=bid_order['price'], amount=bid_order['amount'],
                    filled_qty=match_qty, status=buy_status, timestamp=timestamp
                )

                # B. Cho người Bán
                sell_status = "FILLED" if sell_rem <= 1e-8 else "PARTIALLY_FILLED"
                save_order_history(
                    pipe=pipe, order_id=ask_order['order_id'], user_id=ask_order['user_id'],
                    symbol=symbol, side="sell", price=ask_order['price'], amount=ask_order['amount'],
                    filled_qty=match_qty, status=sell_status, timestamp=timestamp
                )

                pipe.execute()

            except Exception as e:
                logging.error(f"P2P Match Error: {e}")
                break
        else:
            break

    # --- PHASE 2: KHỚP REAL ---
    # A. User MUA vs Real BÁN
    while True:
        best_bid_ids = redis_client.zrange(v_bids_key, -1, -1)
        if not best_bid_ids: break
        
        bid_order = get_order_details(best_bid_ids[0])
        if not bid_order: 
            redis_client.zrem(v_bids_key, best_bid_ids[0])
            continue

        real_asks = get_orderbook(symbol, type="real", side="asks")
        if not real_asks or "asks" not in real_asks or not real_asks["asks"]: break
        
        min_real_price = real_asks["asks"][0][0]

        if bid_order['price'] >= min_real_price:
            match_qty = bid_order['remaining_amount']
            logging.info(f"🌊 REAL MATCH (BUY): {match_qty} {symbol} @ {min_real_price}")
            
            try:
                # Gọi Lua (Real Match khớp hết lệnh User luôn)
                res_lua = redis_client.evalsha(
                    real_sha, 3,
                    f"order:virtual:{bid_order['order_id']}",
                    f"user:{bid_order['user_id']}:balance",
                    v_bids_key,
                    match_qty, min_real_price, bid_order['order_id'], 'bids'
                )
                
                timestamp = time.time()
                pipe = redis_client.pipeline()

                # Ghi Trade
                save_trade_log(
                    pipe=pipe, symbol=symbol, price=min_real_price, amount=match_qty,
                    taker_side="buy", buyer_id=bid_order['user_id'], seller_id="3", timestamp=timestamp
                )

                # Ghi Order History (User Mua - Chắc chắn FILLED vì Real bao thanh khoản)
                # Tuy nhiên để an toàn, check res_lua (remaining)
                rem = float(res_lua)
                status = "FILLED" if rem <= 1e-8 else "PARTIALLY_FILLED"
                
                save_order_history(
                    pipe=pipe, order_id=bid_order['order_id'], user_id=bid_order['user_id'],
                    symbol=symbol, side="buy", price=bid_order['price'], amount=bid_order['amount'],
                    filled_qty=match_qty, status=status, timestamp=timestamp
                )
                
                pipe.execute()

            except Exception as e:
                logging.error(f"Real Match Buy Error: {e}")
                break
        else:
            break 

    # B. User BÁN vs Real MUA
    while True:
        best_ask_ids = redis_client.zrange(v_asks_key, 0, 0)
        if not best_ask_ids: break
        
        ask_order = get_order_details(best_ask_ids[0])
        if not ask_order:
            redis_client.zrem(v_asks_key, best_ask_ids[0])
            continue

        real_bids = get_orderbook(symbol, type="real", side="bids")
        if not real_bids or "bids" not in real_bids or not real_bids["bids"]: break
    
        max_real_price = real_bids["bids"][0][0]

        if ask_order['price'] <= max_real_price:
            match_qty = ask_order['remaining_amount']
            logging.info(f"🌊 REAL MATCH (SELL): {match_qty} {symbol} @ {max_real_price}")
            
            try:
                res_lua = redis_client.evalsha(
                    real_sha, 3,
                    f"order:virtual:{ask_order['order_id']}",
                    f"user:{ask_order['user_id']}:balance",
                    v_asks_key,
                    match_qty, max_real_price, ask_order['order_id'], 'asks'
                )

                timestamp = time.time()
                pipe = redis_client.pipeline()

                save_trade_log(
                    pipe=pipe, symbol=symbol, price=max_real_price, amount=match_qty,
                    taker_side="sell", buyer_id="3", seller_id=ask_order['user_id'], timestamp=timestamp
                )

                rem = float(res_lua)
                status = "FILLED" if rem <= 1e-8 else "PARTIALLY_FILLED"

                save_order_history(
                    pipe=pipe, order_id=ask_order['order_id'], user_id=ask_order['user_id'],
                    symbol=symbol, side="sell", price=ask_order['price'], amount=ask_order['amount'],
                    filled_qty=match_qty, status=status, timestamp=timestamp
                )
                
                pipe.execute()
                
            except Exception as e:
                logging.error(f"Real Match Sell Error: {e}")
                break
        else:
            break