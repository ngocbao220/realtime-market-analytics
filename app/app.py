from pipeline.trades_pipeline import start_trades_pipeline
from pipeline.tickers_pipeline import start_tickers_pipeline
from pipeline.orderbook_pipeline import start_orderbook_pipeline

from pyspark.sql import SparkSession
if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("BinanceStreaming")
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    
    start_trades_pipeline(spark)
    start_tickers_pipeline(spark)
    start_orderbook_pipeline(spark)

    spark.streams.awaitAnyTermination()
