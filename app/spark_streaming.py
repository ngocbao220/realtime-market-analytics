from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder \
    .appName("RiskStreamProcessor") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

schema = StructType([
    StructField("ticker", StringType()),
    StructField("date", StringType()),
    StructField("accounting", DoubleType()),
    StructField("misstatement", DoubleType()),
    StructField("events", DoubleType()),
    StructField("risk", DoubleType()),
    StructField("risk_ind_adjs", DoubleType())
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "risk_signals") \
    .option("startingOffsets", "earliest") \
    .load()

values = kafka_df.selectExpr("CAST(value AS STRING) as json")
parsed = values.select(from_json(col("json"), schema).alias("data")).select("data.*")

# 🔍 Phân loại rủi ro dựa trên risk/risk_ind_adjs
scored = parsed.withColumn(
    "risk_level",
    when(col("risk_ind_adjs") > 0.8, "HIGH")
    .when(col("risk_ind_adjs") > 0.5, "MEDIUM")
    .otherwise("LOW")
)

def write_to_cassandra(df, epoch_id):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="risk_signals", keyspace="op_risk") \
        .save()

query = scored.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/risk_stream") \
    .start()

query.awaitTermination()
