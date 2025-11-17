from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import traceback

def write_clickhouse_batch(
    df: DataFrame, 
    batch_id: int,
    table_name: str, 
    host: str = "clickhouse", 
    port: int = 8123, 
    user: str = "default", 
    password: str = "12345",
    database: str = "default"
) -> None:
    """
    Ghi từng micro-batch của streaming DataFrame vào ClickHouse.
    """
    
    try:
        print(f"\n{'='*60}")
        print(f"[Batch {batch_id}] Starting to process...")
        print(f"{'='*60}")
        
        # Kiểm tra batch rỗng
        count = df.count()
        if count == 0:
            print(f"[Batch {batch_id}] Empty batch, skipping...")
            return
        
        print(f"[Batch {batch_id}] Processing {count} records")
        
        # Print schema lần đầu
        if batch_id == 0:
            print("\n📊 DataFrame Schema:")
            df.printSchema()
        
        # Show sample data
        print(f"\n[Batch {batch_id}] Sample data (3 rows):")
        df.show(3, truncate=False)
        
        # ĐẢM BẢO THỨ TỰ CỘT ĐÚNG VỚI CLICKHOUSE TABLE
        df_ordered = df.select(
            "Symbol",
            "TradeID",
            "Price",
            "Quantity",
            "EventTime",
            "TradeTime",
            "IsBuyerMaker",  # Đã là Int (0/1)
            "Side",
            "TradeValue",
            "Year",
            "Month",
            "Day",
            "Hour"
        )
        
        # Verify data types
        if batch_id == 0:
            print("\n🔍 Column types after ordering:")
            for field in df_ordered.schema.fields:
                print(f"  {field.name}: {field.dataType}")
        
        clickhouse_url = f"jdbc:clickhouse://{host}:{port}/{database}"
        
        connection_properties = {
            "user": user,
            "password": password,
            "driver": "com.clickhouse.jdbc.ClickHouseDriver",
            "batchsize": "10000",
            "socket_timeout": "300000",
            "connect_timeout": "60000",
            "rewriteBatchedStatements": "true"
        }
        
        print(f"\n[Batch {batch_id}] 📤 Writing to ClickHouse...")
        print(f"  URL: {clickhouse_url}")
        print(f"  Table: {table_name}")
        print(f"  Records: {count}")
        
        # Ghi vào ClickHouse
        df_ordered.write \
            .jdbc(
                url=clickhouse_url,
                table=table_name,
                mode="append",
                properties=connection_properties
            )
        
        print(f"\n[Batch {batch_id}] ✅ SUCCESS! Wrote {count} records to ClickHouse")
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"\n[Batch {batch_id}] ❌ ERROR writing to ClickHouse:")
        print(f"{'='*60}")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        print(f"{'='*60}")
        traceback.print_exc()
        
        # KHÔNG raise exception để stream không bị dừng
        # Nếu muốn dừng stream khi có lỗi, uncomment dòng dưới:
        # raise