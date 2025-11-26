# --- LUA SCRIPT 1: KHỚP P2P (User vs User) ---
# Logic: Trừ reserved của cả 2, cộng available cho đối phương.
# Cập nhật remaining_amount, nếu hết thì xóa khỏi ZSET.
LUA_MATCH_P2P = """
    -- KEYS: [1] BuyOrderKey, [2] SellOrderKey, [3] BuyerBalKey, [4] SellerBalKey, [5] BuyBookZSet, [6] SellBookZSet
    -- ARGV: [1] MatchQty, [2] MatchPrice, [3] BuyOrderID, [4] SellOrderID

    local buy_oid_key = KEYS[1]
    local sell_oid_key = KEYS[2]
    local buyer_bal_key = KEYS[3]
    local seller_bal_key = KEYS[4]
    local buy_zset = KEYS[5]
    local sell_zset = KEYS[6]

    local qty = tonumber(ARGV[1])
    local price = tonumber(ARGV[2])
    local buy_oid = ARGV[3]
    local sell_oid = ARGV[4]
    
    local total_cost = qty * price

    -- 1. XỬ LÝ TIỀN (BALANCE)
    redis.call('HINCRBYFLOAT', buyer_bal_key, 'reserved_usd', -total_cost)
    redis.call('HINCRBYFLOAT', buyer_bal_key, 'btc', qty)
    
    redis.call('HINCRBYFLOAT', seller_bal_key, 'reserved_btc', -qty)
    redis.call('HINCRBYFLOAT', seller_bal_key, 'usd', total_cost)

    -- 2. CẬP NHẬT LỆNH MUA (BUY ORDER) - [FIXED: Thêm tonumber]
    local buy_rem = tonumber(redis.call('HINCRBYFLOAT', buy_oid_key, 'remaining_amount', -qty))
    redis.call('HINCRBYFLOAT', buy_oid_key, 'filled_amount', qty)
    
    if buy_rem <= 0.00000001 then
        redis.call('HSET', buy_oid_key, 'status', 'FILLED')
        redis.call('ZREM', buy_zset, buy_oid)
    else
        redis.call('HSET', buy_oid_key, 'status', 'PARTIALLY_FILLED')
    end

    -- 3. CẬP NHẬT LỆNH BÁN (SELL ORDER) - [FIXED: Thêm tonumber]
    local sell_rem = tonumber(redis.call('HINCRBYFLOAT', sell_oid_key, 'remaining_amount', -qty))
    redis.call('HINCRBYFLOAT', sell_oid_key, 'filled_amount', qty)
    
    if sell_rem <= 0.00000001 then
        redis.call('HSET', sell_oid_key, 'status', 'FILLED')
        redis.call('ZREM', sell_zset, sell_oid)
    else
        redis.call('HSET', sell_oid_key, 'status', 'PARTIALLY_FILLED')
    end

    return {buy_rem, sell_rem}
"""

# --- LUA SCRIPT 2: KHỚP REAL (User vs System) ---
LUA_MATCH_REAL = """
    -- KEYS: [1] UserOrderKey, [2] UserBalKey, [3] OrderBookZSet
    -- ARGV: [1] MatchQty, [2] MatchPrice, [3] OrderID, [4] Side ('bids' or 'asks')

    local order_key = KEYS[1]
    local bal_key = KEYS[2]
    local zset_key = KEYS[3]

    local qty = tonumber(ARGV[1])
    local price = tonumber(ARGV[2])
    local order_id = ARGV[3]
    local side = ARGV[4]
    
    local total_cost = qty * price

    -- 1. XỬ LÝ TIỀN USER & SYSTEM
    if side == 'bids' then
        redis.call('HINCRBYFLOAT', bal_key, 'reserved_usd', -total_cost)
        redis.call('HINCRBYFLOAT', bal_key, 'btc', qty)
        
        redis.call('HINCRBYFLOAT', 'user:3:balance', 'usd', total_cost)
        redis.call('HINCRBYFLOAT', 'user:3:balance', 'btc', -qty)
    else
        redis.call('HINCRBYFLOAT', bal_key, 'reserved_btc', -qty)
        redis.call('HINCRBYFLOAT', bal_key, 'usd', total_cost)
        
        redis.call('HINCRBYFLOAT', 'user:3:balance', 'btc', qty)
        redis.call('HINCRBYFLOAT', 'user:3:balance', 'usd', -total_cost)
    end

    -- 2. CẬP NHẬT LỆNH USER - [FIXED: Thêm tonumber]
    local rem = tonumber(redis.call('HINCRBYFLOAT', order_key, 'remaining_amount', -qty))
    redis.call('HINCRBYFLOAT', order_key, 'filled_amount', qty)
    
    if rem <= 0.00000001 then
        redis.call('HSET', order_key, 'status', 'FILLED')
        redis.call('ZREM', zset_key, order_id)
    else
        redis.call('HSET', order_key, 'status', 'PARTIALLY_FILLED')
    end

    return rem
"""