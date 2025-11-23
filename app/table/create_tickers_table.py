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
    client.execute(f"""
        CREATE OR REPLACE VIEW tickers_indicators AS
        SELECT
            symbol,
            event_time,
            close_price,
            volume,
            
            -- Tính MA7 (Trung bình 7 điểm gần nhất)
            avg(close_price) OVER (
                PARTITION BY symbol 
                ORDER BY event_time 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS ma7,

            -- Tính MA25
            avg(close_price) OVER (
                PARTITION BY symbol 
                ORDER BY event_time 
                ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
            ) AS ma25,

            -- Tính MA99
            avg(close_price) OVER (
                PARTITION BY symbol 
                ORDER BY event_time 
                ROWS BETWEEN 98 PRECEDING AND CURRENT ROW
            ) AS ma99

        FROM tickers;
    """)
    print("View 'tickers_indicators' checked/created/updated.")
    
    client.execute(f"""
        CREATE OR REPLACE VIEW tickers_ohlcv_1m AS
        SELECT
            symbol,
            toStartOfMinute(event_time) AS time_bucket,
            
            argMin(open_price, event_time) AS open,
            max(high_price)                AS high,
            min(low_price)                 AS low,
            argMax(close_price, event_time) AS close,
            
            sum(volume)                    AS volume,
            sum(quote_volume)              AS quote_volume,
            count()                        AS tick_count

        FROM tickers
        GROUP BY symbol, time_bucket
        ORDER BY time_bucket DESC;
    """)
    print("View 'tickers_ohlcv_1m' checked/created/updated.")