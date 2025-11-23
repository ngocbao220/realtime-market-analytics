from consumer.kafka_reader import read_kafka_stream
from transform.orderbook_transform import transform_orderbook
from sinks.clickhouse_writer import write_clickhouse_batch
from sinks.redis_writer import write_orderbook_to_redis
from table.create_orderbook_table import create_clickhouse_table_orderbook
from config.setting import *

import os

def start_orderbook_pipeline(spark):
    # 1. Read from Kafka
    df_raw = read_kafka_stream(spark, KAFKA_BROKER, TOPIC_ORDERBOOK)
    df_clean = transform_orderbook(df_raw)
    
    # Tạo bảng ClickHouse nếu chưa có (Chạy 1 lần ở driver)
    try:
        create_clickhouse_table_orderbook(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )

        print(f"Create successfuly table orderbook")
    except Exception as e:
        print(f"⚠️ Warning creating table: {e}")

    # --- HÀM XỬ LÝ TRUNG TÂM (MASTER BATCH) ---
    # Hàm này sẽ chạy trên mỗi Micro-batch dữ liệu
    def process_master_batch(batch_df, batch_id):
        print(f"⚡ Processing Batch ID: {batch_id} - Size: {batch_df.count()}")
        
        # Cache lại batch_df vì ta sẽ dùng nó 2 lần (Action 1: Redis, Action 2: ClickHouse)
        # Nếu không cache, Spark có thể tính toán lại dataframe này 2 lần.
        batch_df.persist() 
        
        try:
            # -------------------------------------------------
            # NHIỆM VỤ 1: GHI SANG REDIS (Hot Data)
            # -------------------------------------------------
            # Gọi hàm logic ghi Redis (Hàm này không được chứa .writeStream)
            # Bạn cần sửa file redis_writer.py để expose hàm process_partition ra, hoặc viết inline ở đây
            # Ví dụ gọi hàm wrapper:
            write_orderbook_to_redis(batch_df) 
            print("✅ Redis Update Orderbook Done")

            # -------------------------------------------------
            # NHIỆM VỤ 2: GHI SANG CLICKHOUSE (Cold Data)
            # -------------------------------------------------
            write_clickhouse_batch(
                batch_df,
                batch_id,
                table_name="orderbook",
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DATABASE,
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT
            )
            print("✅ ClickHouse Insert Orderbook Done")
            
        except Exception as e:
            print(f"❌ Error in batch {batch_id}: {e}")
        finally:
            # Giải phóng bộ nhớ
            batch_df.unpersist()

    checkpoint_path = os.path.join(CHECKPOINT_DIR, "orderbook")
    
    query = (
        df_clean.writeStream
        .queryName("Query_Orderbook")
        .trigger(processingTime=PROCESSING_TIME)
        .foreachBatch(process_master_batch)
        .option("checkpointLocation", checkpoint_path) # <--- Dùng đường dẫn riêng
        .start()
    )

    return query