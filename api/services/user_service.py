from db import redis_client

def create_user(username: str):
    user_id = redis_client.incr("user:id:counter")
    
    # Lưu info cơ bản
    redis_client.hset(f"user:{user_id}", mapping={
        "user_id": user_id,
        "username": username,
    })

    # Lưu balance riêng
    redis_client.hset(f"user:{user_id}:balance", mapping={
        "usd": 1000.0,
        "btc": 1.0,
        "reserved_usd": 0.0,
        "reserved_btc": 0.0
    })

    return {
        "user_id": user_id,
        "username": username,
        "role": "user",
        "usd": 1000.0,
        "btc": 1.0,
        "reserved_usd": 0.0,
        "reserved_btc": 0.0
    }

def get_user(user_id: str):
    info = redis_client.hgetall(f"user:{user_id}")
    balance = redis_client.hgetall(f"user:{user_id}:balance")

    if not info or not balance:
        return {"error": "User not found"}

    # Convert balance sang float
    def safe_float(v):
        try:
            return float(v)
        except:
            return 0.0

    return {
        "user_id": info.get("user_id"),
        "username": info.get("username"),
        "role": info.get("role", "user"),
        "usd": safe_float(balance.get("usd")),
        "btc": safe_float(balance.get("btc")),
        "reserved_usd": safe_float(balance.get("reserved_usd")),
        "reserved_btc": safe_float(balance.get("reserved_btc")),
    }

def get_all_users():
    # 1. Lấy ID lớn nhất hiện tại
    max_id = redis_client.get("user:id:counter")
    if not max_id:
        return []
    
    max_id = int(max_id)
    
    # 2. Dùng Pipeline để gom lệnh (Tối ưu tốc độ, tránh nghẽn mạng)
    pipe = redis_client.pipeline()
    
    for i in range(1, max_id + 1):
        pipe.hgetall(f"user:{i}")          # Lấy thông tin cơ bản
        pipe.hgetall(f"user:{i}:balance")  # Lấy thông tin số dư
        
    # Thực thi 1 lần duy nhất cho tất cả lệnh trên
    results = pipe.execute()
    
    # 3. Xử lý kết quả trả về
    users = []
    
    # Hàm con để convert số an toàn
    def safe_float(v):
        try: return float(v)
        except: return 0.0

    # Results sẽ trả về xen kẽ: [info_1, bal_1, info_2, bal_2, ...]
    # Bước nhảy là 2 (step=2)
    for i in range(0, len(results), 2):
        info = results[i]
        balance = results[i+1]
        
        # Kiểm tra kỹ xem user có dữ liệu không (để tránh user bị xóa hoặc lỗi)
        if info and "user_id" in info:
            users.append({
                "user_id": info.get("user_id"),
                "username": info.get("username"),
                "role": info.get("role", "user"),
                "usd": safe_float(balance.get("usd")),
                "btc": safe_float(balance.get("btc")),
                "reserved_usd": safe_float(balance.get("reserved_usd")),
                "reserved_btc": safe_float(balance.get("reserved_btc")),
            })
            
    return users

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True
)

def init_admin_account():

    """Hàm này sẽ được chạy khi server khởi động"""
    admin_id = "0"
    username = "admin"
    
    # Kiểm tra xem admin đã tồn tại chưa
    if redis_client.exists(f"user:{admin_id}"):
        logging.info("✅ Admin account already exists.")
        return

    logging.info("⚙️ Creating default Admin account...")

    redis_client.hset(f"user:{admin_id}", mapping={
        "user_id": admin_id,
        "username": username,
        "role": "admin" # Đánh dấu đây là admin
    })

    redis_client.hset(f"user:{admin_id}:balance", mapping={
        "usd": 1000000000.0,
        "btc": 1000.0,
        "reserved_usd": 0.0,
        "reserved_btc": 0.0
    })


def delete_user_service(user_id: str):
    """Xóa user khỏi hệ thống (Trừ Admin)"""
    
    # 1. CHẶN KHÔNG CHO XÓA ADMIN
    if str(user_id) == "0":
        return {"success": False, "message": "Không thể xóa tài khoản Admin!"}

    user_key = f"user:{user_id}"
    balance_key = f"user:{user_id}:balance"

    # Kiểm tra xem user có tồn tại không
    if not redis_client.exists(user_key):
        return {"success": False, "message": "User ID không tồn tại."}

    # 2. Xóa dữ liệu (Dùng pipeline để xóa sạch cả 2 key cùng lúc)
    pipe = redis_client.pipeline()
    pipe.delete(user_key)
    pipe.delete(balance_key)
    pipe.execute()

    return {"success": True, "message": f"Đã xóa User ID {user_id}"}