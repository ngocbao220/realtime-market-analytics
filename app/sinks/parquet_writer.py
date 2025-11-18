from config.setting import PROCESSING_TIME
def write_parquet_stream(df, path, checkpoint, partition_cols=None):
    writer = (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
    )

    # Nếu có partition columns → unpack danh sách
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    query = (
        writer
        .trigger(processingTime=PROCESSING_TIME)
        .start()
    )

    return query
