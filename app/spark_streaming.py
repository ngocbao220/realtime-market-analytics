from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, from_json, year, month, dayofmonth, hour,
    lit, posexplode, when, sum as _sum, avg, stddev, count,
    window, lag, expr
)
from pyspark.sql.types import *
from pyspark.sql.window import Window

# ======================================================
# KHỞI TẠO SPARK SESSION
# ======================================================
spark = (
    SparkSession.builder
    .appName("BinanceProcessing_WithIndicators")
    .config("spark.sql.caseSensitive", "true")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ======================================================
# CẤU HÌNH
# ======================================================
KAFKA_BROKER = "kafka:9092"
TOPIC_TRADES = "binance_trades"
TOPIC_TICKERS = "binance_tickers_1h"
TOPIC_ORDERBOOK = "binance_orderbook"

# Đường dẫn lưu Parquet
OUTPUT_PATH = "/data/processed"
CHECKPOINT_DIR = "/checkpoints"

# ======================================================
# ĐỊNH NGHĨA SCHEMAS
# ======================================================
trade_schema = StructType([
    StructField("e", StringType()),
    StructField("E", LongType()),
    StructField("s", StringType()),
    StructField("t", LongType()),
    StructField("p", StringType()),
    StructField("q", StringType()),
    StructField("b", LongType()),
    StructField("a", LongType()),
    StructField("T", LongType()),
    StructField("m", BooleanType()),
    StructField("M", BooleanType())
])

ticker_schema = StructType([
    StructField("e", StringType()),
    StructField("E", LongType()),
    StructField("s", StringType()),
    StructField("O", LongType()),
    StructField("C", LongType()),
    StructField("o", StringType()),
    StructField("h", StringType()),
    StructField("l", StringType()),
    StructField("c", StringType()),
    StructField("v", StringType()),
    StructField("q", StringType())
])

orderbook_schema = StructType([
    StructField("e", StringType()),
    StructField("E", LongType()),
    StructField("s", StringType()),
    StructField("b", ArrayType(ArrayType(StringType()))),
    StructField("a", ArrayType(ArrayType(StringType())))
])

# ======================================================
# PHASE 3.1: CLEAN & TRANSFORM TRADES
# ======================================================
print("🔄 Phase 3.1: Clean & Transform Trades...")

trades_raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_TRADES)
    .option("startingOffsets", "earliest")
    .load()
)

# Clean & Transform với validation
trades_cleaned_df = (
    trades_raw_df
    .select(from_json(col("value").cast("string"), trade_schema).alias("data"))
    .filter(col("data").isNotNull())
    .select(
        col("data.s").alias("Symbol"),
        col("data.t").alias("TradeID"),
        col("data.p").cast(DoubleType()).alias("Price"),
        col("data.q").cast(DoubleType()).alias("Quantity"),
        (col("data.E") / 1000).cast("timestamp").alias("EventTime"),
        (col("data.T") / 1000).cast("timestamp").alias("TradeTime"),
        col("data.m").alias("IsBuyerMaker")
    )
    .withColumn("Side", when(col("IsBuyerMaker") == True, "SELL").otherwise("BUY"))
    .withColumn("TradeValue", col("Price") * col("Quantity"))
    .filter(col("Price") > 0)
    .filter(col("Quantity") > 0)
    .withColumn("Year", year(col("TradeTime")))
    .withColumn("Month", month(col("TradeTime")))
    .withColumn("Day", dayofmonth(col("TradeTime")))
    .withColumn("Hour", hour(col("TradeTime")))
)

# DEBUG: Disabled - data parse successfully verified
# debug_parsed = (
#     trades_cleaned_df
#     .select("Symbol", "TradeID", "Price", "Quantity", "TradeTime", "Side")
#     .writeStream
#     .format("console")
#     .outputMode("append")
#     .option("truncate", "false")
#     .trigger(processingTime="10 seconds")
#     .start()
# )

# ======================================================
# PHASE 3.2: BATCH TRADES → PARQUET (PARTITIONED)
# ======================================================
print("💾 Phase 3.2: Writing Trades to Parquet...")

query_trades_parquet = (
    trades_cleaned_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", f"{OUTPUT_PATH}/trades")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/trades_parquet")
    .partitionBy("Symbol", "Year", "Month", "Day", "Hour")
    .trigger(processingTime="5 seconds")
    .start()
)

# ======================================================
# PHASE 3.3: CLEAN & TRANSFORM TICKERS + INDICATORS
# ======================================================
print("🔄 Phase 3.3: Clean & Transform Tickers with Indicators...")

tickers_raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_TICKERS)
    .option("startingOffsets", "earliest")
    .load()
)

tickers_cleaned_df = (
    tickers_raw_df
    .select(from_json(col("value").cast("string"), ticker_schema).alias("data"))
    .filter(col("data").isNotNull())
    .select(
        col("data.s").alias("Symbol"),
        to_timestamp(col("data.O") / 1000).alias("OpenTime"),
        to_timestamp(col("data.C") / 1000).alias("CloseTime"),
        col("data.o").cast(DoubleType()).alias("Open"),
        col("data.h").cast(DoubleType()).alias("High"),
        col("data.l").cast(DoubleType()).alias("Low"),
        col("data.c").cast(DoubleType()).alias("Close"),
        col("data.v").cast(DoubleType()).alias("BaseVolume"),
        col("data.q").cast(DoubleType()).alias("QuoteVolume"),
        to_timestamp(col("data.E") / 1000).alias("EventTime")
    )
    .filter(col("Close") > 0)
    .withColumn("Year", year(col("CloseTime")))
    .withColumn("Month", month(col("CloseTime")))
    .withColumn("Day", dayofmonth(col("CloseTime")))
    .withColumn("Hour", hour(col("CloseTime")))
)

# Technical Indicators (sẽ tính trong batch processing hoặc micro-batch)
# Note: Streaming không support Window functions với lag/lead
# Cần tính sau khi ghi vào Parquet

# ======================================================
# PHASE 3.4: BATCH TICKERS → PARQUET
# ======================================================
print("💾 Phase 3.4: Writing Tickers to Parquet...")

query_tickers_parquet = (
    tickers_cleaned_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", f"{OUTPUT_PATH}/tickers")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/tickers_parquet")
    .partitionBy("Symbol", "Year", "Month", "Day")
    .trigger(processingTime="5 seconds")
    .start()
)

# ======================================================
# PHASE 3.5: ORDERBOOK STATISTICS
# ======================================================
print("🔄 Phase 3.5: Orderbook Statistics...")

orderbook_raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_ORDERBOOK)
    .option("startingOffsets", "earliest")
    .load()
)

parsed_orderbook_df = (
    orderbook_raw_df
    .select(from_json(col("value").cast("string"), orderbook_schema).alias("data"))
    .filter(col("data").isNotNull())
    .select(
        col("data.s").alias("Symbol"),
        to_timestamp(col("data.E") / 1000).alias("EventTime"),
        col("data.b").alias("Bids"),
        col("data.a").alias("Asks")
    )
)

# Explode Bids with Level
bids_df = parsed_orderbook_df.select(
    "Symbol", "EventTime", posexplode(col("Bids")).alias("Level0", "Bid")
).select(
    col("Symbol"),
    col("EventTime"),
    lit("BID").alias("Side"),
    col("Bid")[0].cast(DoubleType()).alias("Price"),
    col("Bid")[1].cast(DoubleType()).alias("Qty"),
    (col("Level0") + 1).alias("Level")
).filter(col("Price") > 0).filter(col("Qty") > 0)

# Explode Asks with Level
asks_df = parsed_orderbook_df.select(
    "Symbol", "EventTime", posexplode(col("Asks")).alias("Level0", "Ask")
).select(
    col("Symbol"),
    col("EventTime"),
    lit("ASK").alias("Side"),
    col("Ask")[0].cast(DoubleType()).alias("Price"),
    col("Ask")[1].cast(DoubleType()).alias("Qty"),
    (col("Level0") + 1).alias("Level")
).filter(col("Price") > 0).filter(col("Qty") > 0)

# Union orderbook
orderbook_df = bids_df.unionByName(asks_df)

# Simplified orderbook - NO AGGREGATION to avoid checkpoint issues
orderbook_simple_df = (
    orderbook_df
    .withColumn("Year", year(col("EventTime")))
    .withColumn("Month", month(col("EventTime")))
    .withColumn("Day", dayofmonth(col("EventTime")))
    .withColumn("Hour", hour(col("EventTime")))
)

# ======================================================
# PHASE 3.6: BATCH ORDERBOOK → PARQUET (SIMPLIFIED - NO AGGREGATION)
# ======================================================
print("💾 Phase 3.6: Writing Orderbook to Parquet (Simplified)...")

query_orderbook_parquet = (
    orderbook_simple_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", f"{OUTPUT_PATH}/orderbook")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/orderbook_simple")
    .partitionBy("Symbol", "Year", "Month", "Day")
    .trigger(processingTime="5 seconds")
    .start()
)

# ======================================================
# CONSOLE OUTPUT FOR MONITORING
# ======================================================
query_trades_console = (
    trades_cleaned_df
    .select("Symbol", "Price", "Quantity", "Side", "TradeValue", "TradeTime")
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/trades_console")
    .trigger(processingTime="5 seconds")
    .start()
)

query_orderbook_console = (
    orderbook_simple_df
    .select("Symbol", "EventTime", "Side", "Price", "Qty", "Level")
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/orderbook_console")
    .trigger(processingTime="5 seconds")
    .start()
)

print("✅ All streams started successfully!")
print(f"📂 Data will be saved to: {OUTPUT_PATH}")
print("=" * 80)

spark.streams.awaitAnyTermination()
