from db import redis_client
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

INF_USD = 999999999.0
INF_COIN = 999999999.0

# --- CÁC KEY REDIS ---
KEY_USER_PREFIX = "user"          # Hash: user:{id}
KEY_USER_ID_COUNTER = "sys:user_id_seq" # String: Bộ đếm ID
KEY_USERNAME_LOOKUP = "sys:usernames"   # Hash: username -> user_id (Để check trùng nhanh)
KEY_ACTIVE_USERS = "sys:active_users"   # Set: Lưu các user_id đang tồn tại (Để get all nhanh)

def safe_float(val):
    try: return float(val)
    except: return 0.0

def init_special_account():
    """
    Khởi tạo Admin (0), Marker Bot (1), Taker Bot (2).
    Hàm này nên chạy 1 lần khi server start.
    """
    # Nếu đã có admin thì coi như đã init xong, bỏ qua
    if redis_client.hexists(KEY_USERNAME_LOOKUP, "admin"):
        logging.info("✅ Special accounts already exist.")
        return

    logging.info("⚙️ Initializing special accounts...")
    
    pipe = redis_client.pipeline()

    # 1. ADMIN (ID 0)
    pipe.hset(f"{KEY_USER_PREFIX}:0", mapping={
        "user_id": "0", "username": "admin", "role": "admin",
        "usd": INF_USD, "btc": INF_COIN, "reserved_usd": 0, "reserved_btc": 0
    })
    pipe.hset(KEY_USERNAME_LOOKUP, "admin", "0")
    pipe.sadd(KEY_ACTIVE_USERS, "0")

    # 2. MARKER BOT (ID 1)
    pipe.hset(f"{KEY_USER_PREFIX}:1", mapping={
        "user_id": "1", "username": "marker_bot", "role": "bot",
        "usd": INF_USD, "btc": INF_COIN, "reserved_usd": 0, "reserved_btc": 0
    })
    pipe.hset(KEY_USERNAME_LOOKUP, "marker_bot", "1")
    pipe.sadd(KEY_ACTIVE_USERS, "1")

    # 3. TAKER BOT (ID 2)
    pipe.hset(f"{KEY_USER_PREFIX}:2", mapping={
        "user_id": "2", "username": "taker_bot", "role": "bot",
        "usd": INF_USD, "btc": INF_COIN, "reserved_usd": 0, "reserved_btc": 0
    })
    pipe.hset(KEY_USERNAME_LOOKUP, "taker_bot", "2")
    pipe.sadd(KEY_ACTIVE_USERS, "2")

    # 4. Set bộ đếm ID bắt đầu từ 2 (để user mới sẽ là 3)
    pipe.set(KEY_USER_ID_COUNTER, 2)

    pipe.execute()
    logging.info("✅ Special accounts created successfully.")


def create_new_user(username: str):
    """
    Tạo user mới. Tối ưu check trùng O(1).
    """
    username = username.strip()
    
    # 1. Kiểm tra nhanh xem username đã tồn tại chưa (O(1))
    existing_id = redis_client.hget(KEY_USERNAME_LOOKUP, username)
    if existing_id:
        # Nếu đã tồn tại, trả về thông tin cũ
        return get_user_balance(existing_id)

    # 2. Tạo User mới (Atomic)
    # Tăng ID trước
    new_id = redis_client.incr(KEY_USER_ID_COUNTER)
    user_key = f"{KEY_USER_PREFIX}:{new_id}"
    
    new_user_data = {
        "user_id": str(new_id),
        "username": username,
        "role": "user",
        "usd": 1000.0,
        "btc": 1.0,
        "reserved_usd": 0.0,
        "reserved_btc": 0.0
    }

    # Dùng Pipeline để đảm bảo tính toàn vẹn dữ liệu
    pipe = redis_client.pipeline()
    pipe.hset(user_key, mapping=new_user_data)       # Lưu data
    pipe.hset(KEY_USERNAME_LOOKUP, username, new_id) # Đánh dấu username đã dùng
    pipe.sadd(KEY_ACTIVE_USERS, new_id)              # Thêm vào danh sách active
    pipe.execute()
    
    logging.info(f"👤 Created new user: {username} (ID: {new_id})")
    return new_user_data


def get_user_balance(user_id: str):
    """
    Lấy thông tin user.
    """
    user_key = f"{KEY_USER_PREFIX}:{user_id}"
    
    # Kiểm tra tồn tại nhanh
    if not redis_client.exists(user_key):
        return None 
        
    data = redis_client.hgetall(user_key)
    
    # Convert số liệu an toàn
    if data:
        data["usd"] = safe_float(data.get("usd"))
        data["btc"] = safe_float(data.get("btc"))
        data["reserved_usd"] = safe_float(data.get("reserved_usd"))
        data["reserved_btc"] = safe_float(data.get("reserved_btc"))
    
    return data


def get_all_users_logic():
    """
    Lấy tất cả user. Tối ưu: Chỉ quét các ID có trong Set active.
    """
    # 1. Lấy tất cả user_id đang hoạt động (không cần đoán max_id)
    active_ids = redis_client.smembers(KEY_ACTIVE_USERS)
    
    if not active_ids:
        return []
    
    # 2. Pipeline lấy data
    pipe = redis_client.pipeline()
    for uid in active_ids:
        pipe.hgetall(f"{KEY_USER_PREFIX}:{uid}")
    
    results = pipe.execute()
    
    # 3. Format dữ liệu
    users = []
    for data in results:
        if data and "user_id" in data:
            data["usd"] = safe_float(data.get("usd"))
            data["btc"] = safe_float(data.get("btc"))
            # Ẩn các trường reserved khi list all cho nhẹ
            if "reserved_usd" in data: del data["reserved_usd"]
            if "reserved_btc" in data: del data["reserved_btc"]
            users.append(data)
            
    # Sort theo ID cho đẹp (vì Set không có thứ tự)
    users.sort(key=lambda x: int(x["user_id"]))
    return users


def delete_user_logic(user_id: str):
    """
    Xóa user và dọn dẹp các index liên quan.
    """
    # 1. Chặn xóa các tài khoản đặc biệt
    if str(user_id) in ["0", "1", "2"]:
        return {"success": False, "message": "Không thể xóa tài khoản Hệ thống (Admin/Bot)"}
    
    user_key = f"{KEY_USER_PREFIX}:{user_id}"
    
    # Lấy username trước để còn xóa trong bảng Lookup
    username = redis_client.hget(user_key, "username")
    
    if not username:
        return {"success": False, "message": "User không tồn tại"}
    
    try:
        pipe = redis_client.pipeline()
        pipe.delete(user_key)                       # Xóa data
        pipe.hdel(KEY_USERNAME_LOOKUP, username)    # Xóa khỏi bảng check trùng -> Username này có thể dùng lại
        pipe.srem(KEY_ACTIVE_USERS, user_id)        # Xóa khỏi danh sách active
        pipe.execute()
        
        logging.info(f"🗑️ Deleted user {user_id} ({username})")
        return {"success": True, "message": f"Đã xóa user {user_id}"}
    except Exception as e:
        return {"success": False, "message": str(e)}

# --- CHẠY THỬ KHỞI TẠO ---
# Gọi hàm này ở file main.py khi server start
if __name__ == "__main__":
    init_special_account()