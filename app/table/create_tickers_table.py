from clickhouse_driver import Client

def create_clickhouse_table_ticker(
    host,
    port,
    user,
    password,
    database,
):
    client = Client(host=host, port=port, user=user, password=password, database=database)

    client.execute(f"""
        CREATE TABLE IF NOT EXISTS tickers (
            symbol String,        
            event_type String,
            event_time DateTime, 
            open_time DateTime,  
            close_time DateTime, 
            open_price Float64,       
            close_price Float64,
            high_price Float64,       
            low_price Float64,        
            volume Float64,
            quote_volume Float64,
            Year Int32,
            Month UInt32,
            Day UInt32
        )
        ENGINE = MergeTree()
        ORDER BY (event_time, symbol);    
    """)