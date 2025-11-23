from consumer.kafka_reader import read_kafka_stream
from transform.trades_transform import transform_trades
from sinks.clickhouse_writer import write_clickhouse_batch
from sinks.redis_writer import write_redis_batch_logic
from table.create_trades_table import create_clickhouse_table_trade
from config.setting import *

def start_trades_pipeline(spark):
    # 1. Read from Kafka
    df_raw = read_kafka_stream(spark, KAFKA_BROKER, TOPIC_TRADES)
    df_clean = transform_trades(df_raw)
    
    # Tạo bảng ClickHouse nếu chưa có (Chạy 1 lần ở driver)
    try:
        create_clickhouse_table_trade(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
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
            write_redis_batch_logic(batch_df) 
            print("✅ Redis Update Done")

            # -------------------------------------------------
            # NHIỆM VỤ 2: GHI SANG CLICKHOUSE (Cold Data)
            # -------------------------------------------------
            write_clickhouse_batch(
                batch_df,
                batch_id,
                table_name="trades",
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DATABASE,
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT
            )
            print("✅ ClickHouse Insert Done")
            
        except Exception as e:
            print(f"❌ Error in batch {batch_id}: {e}")
        finally:
            # Giải phóng bộ nhớ
            batch_df.unpersist()

    # --- KHỞI CHẠY 1 STREAM DUY NHẤT ---
    query = (
        df_clean.writeStream
        .trigger(processingTime=PROCESSING_TIME)
        .foreachBatch(process_master_batch) # <--- Trái tim của hệ thống
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    return query