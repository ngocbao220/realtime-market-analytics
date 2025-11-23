from clickhouse_driver import Client

def create_clickhouse_table_kline(host, port, user, password, database):
    client = Client(host=host, port=port, user=user, password=password, database=database)

    client.execute(f"""
        CREATE TABLE IF NOT EXISTS klines (
            Symbol String,
            Event_time DateTime64(3),
            Open_time DateTime64(3),
            Close_time DateTime64(3),
            Interval String,
            
            Open Float64,
            High Float64,
            Low Float64,
            Close Float64,
            Volume Float64,
            Quote_volume Float64,
            
            Num_trades UInt64,
            Taker_buy_volume Float64,
            Taker_buy_quote_vol Float64,
            
            Is_closed UInt8,
            Year Int32,
            Month UInt32,
            Day UInt32
        )
        ENGINE = MergeTree()
        ORDER BY (Symbol, Open_time);
    """)
