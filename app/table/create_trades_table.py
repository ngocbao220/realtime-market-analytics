from clickhouse_driver import Client

def create_clickhouse_table_trade(
    host,
    port,
    user,
    password,
    database,
):
    client = Client(host=host, port=port, user=user, password=password, database=database)

    client.execute(f"""
        CREATE TABLE IF NOT EXISTS trades (
            Symbol String,
            TradeID UInt64,
            Price Float64,
            Quantity Float64,
            EventTime DateTime,
            TradeTime DateTime,
            IsBuyerMaker UInt8,
            Side String,
            TradeValue Float64,
            Year Int32,
            Month Int32,
            Day Int32,
            Hour Int32
        )
        ENGINE = MergeTree()
        ORDER BY (TradeTime, Symbol);
    """)
