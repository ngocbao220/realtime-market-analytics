import redis
import json
from config.setting import REDIS_HOST, REDIS_PORT

def write_redis_batch_logic(batch_df):
    
    def process_partition(iterator):
        # Kết nối Redis tại Worker
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        pipe = r.pipeline()
        
        count = 0
        for row in iterator:
            data = row.asDict()
            symbol = data.get("symbol") # Giả sử cột tên là symbol
            price = data.get("price")
            
            if symbol:
                # 1. Update Giá khớp lệnh (cho Engine)
                pipe.set(f"price:{symbol}", price)
                
                # 2. Update thông tin market (cho Dashboard)
                # Convert timestamp thành string để không lỗi JSON
                if "timestamp" in data: data["timestamp"] = str(data["timestamp"])
                pipe.set(f"market:{symbol}", json.dumps(data))
                
            count += 1
            if count % 100 == 0: pipe.execute()
            
        pipe.execute()
        r.close()

    # Phân tán xuống các worker để ghi song song
    batch_df.foreachPartition(process_partition)