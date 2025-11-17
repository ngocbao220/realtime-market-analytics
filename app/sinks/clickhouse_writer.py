from pyspark.sql import DataFrame
import traceback

def write_clickhouse_batch(
    df: DataFrame, 
    batch_id: int,  # Thêm batch_id - BẮT BUỘC cho foreachBatch
    table_name: str, 
    host: str = "clickhouse", 
    port: int = 8123, 
    user: str = "default", 
    password: str = "12345",  # Đổi thành password của bạn
    database: str = "default"
) -> None:
    """
    Ghi từng micro-batch của streaming DataFrame vào ClickHouse.
    
    Args:
        df: DataFrame batch (từ foreachBatch)
        batch_id: ID của batch (bắt buộc cho foreachBatch)
        table_name: tên bảng ClickHouse
        host: ClickHouse host
        port: ClickHouse port
        user: ClickHouse user
        password: ClickHouse password
        database: ClickHouse database
    
    Example:
        query = df.writeStream \\
            .foreachBatch(lambda df, batch_id: write_clickhouse_batch(
                df, batch_id, "trades"
            )) \\
            .start()
    """
    
    try:
        print(f"[Batch {batch_id}] Starting to process...")
        
        # Kiểm tra batch rỗng
        count = df.count()
        if count == 0:
            print(f"[Batch {batch_id}] Empty batch, skipping...")
            return
        
        print(f"[Batch {batch_id}] Processing {count} records")
        
        # Show sample data (chỉ 3 dòng để tránh spam logs)
        print(f"[Batch {batch_id}] Sample data:")
        df.show(3, truncate=False)
        
        # Print schema (chỉ in lần đầu)
        if batch_id == 0:
            print("DataFrame Schema:")
            df.printSchema()
        
        # URL kết nối ClickHouse
        clickhouse_url = f"jdbc:clickhouse://{host}:{port}/{database}"
        
        print(f"[Batch {batch_id}] Writing to ClickHouse table: {table_name}")
        
        # Ghi vào ClickHouse
        df.write \
            .format("jdbc") \
            .option("url", clickhouse_url) \
            .option("dbtable", table_name) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("batchsize", "10000") \
            .option("isolationLevel", "NONE") \
            .option("numPartitions", "4") \
            .mode("append") \
            .save()
        
        print(f"[Batch {batch_id}] Successfully wrote {count} records to ClickHouse")
        
    except Exception as e:
        print(f"[Batch {batch_id}] ERROR writing to ClickHouse:")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        traceback.print_exc()
        
        # KHÔNG raise exception để stream không bị dừng
        # Nếu muốn dừng stream khi có lỗi, uncomment dòng dưới:
        # raise
