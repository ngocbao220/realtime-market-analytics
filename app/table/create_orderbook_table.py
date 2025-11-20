from clickhouse_driver import Client

def create_clickhouse_table_orderbook(
    host,
    port,
    user,
    password,
    database,
):
    client = Client(host=host, port=port, user=user, password=password, database=database)

    # Orderbook cần dùng Array(Float64) để lưu danh sách giá/lượng
    client.execute(f"""
        CREATE TABLE IF NOT EXISTS orderbook (
            symbol String,
            event_time DateTime64(3),
            
            bid_prices Array(Float64),
            bid_quantities Array(Float64),
            
            ask_prices Array(Float64),
            ask_quantities Array(Float64),
            
            Year Int32,
            Month UInt32,
            Day UInt32
        )
        ENGINE = MergeTree()
        ORDER BY (event_time, symbol);
    """)