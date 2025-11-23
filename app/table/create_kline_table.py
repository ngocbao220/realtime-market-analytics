from clickhouse_driver import Client

def create_clickhouse_table_kline(host, port, user, password, database):
    client = Client(host=host, port=port, user=user, password=password, database=database)

    client.execute(f"""
        CREATE TABLE IF NOT EXISTS klines (
            symbol String,
            event_time DateTime64(3),
            open_time DateTime64(3),
            
            -- [FIX] BỔ SUNG 2 CỘT NÀY
            close_time DateTime64(3),
            interval String,
            
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume Float64,
            quote_volume Float64,
            
            num_trades UInt64,
            taker_buy_volume Float64,
            taker_buy_quote_vol Float64,
            
            is_closed UInt8,
            Year Int32,
            Month UInt32,
            Day UInt32
        )
        ENGINE = MergeTree()
        ORDER BY (symbol, open_time);
    """)
