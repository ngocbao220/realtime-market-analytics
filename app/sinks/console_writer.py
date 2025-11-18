from config.setting import *

def write_console_stream(df, table, col_select):
    """
    df: df_clean
    tabel: "trades" or "tickers" ...
    col_select: "Symbol", "Price", "Quantity", "Side", "TradeValue", "TradeTime"
    """
    if col_select == "ALL":
        selected_df = df
    else:
        selected_df = df.select(col_select)
    query_console = (
        selected_df
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{table}_console")
        .trigger(processingTime=PROCESSING_TIME)
        .start()
    )
    return query_console