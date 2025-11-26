import requests
import os
import logging

# --- CẤU HÌNH ---
API_BASE_URL = os.getenv("API_URL", "http://localhost:8000")

# Cấu hình log để debug lỗi kết nối
logging.basicConfig(level=logging.INFO)

# --- HÀM HỖ TRỢ GỌI REQUEST (PRIVATE) ---
def _request(method, endpoint, params=None, json_data=None):
    url = f"{API_BASE_URL}{endpoint}"
    try:
        if method == "GET":
            response = requests.get(url, params=params, timeout=5)
        elif method == "POST":
            response = requests.post(url, json=json_data, timeout=5)
        elif method == "DELETE":
            response = requests.delete(url, timeout=5)
        
        # Trả về JSON nếu thành công (200)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"API Error {endpoint}: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logging.error(f"Connection Error {endpoint}: {e}")
        return None

# ==========================================
# 1. MARKET DATA (Kline, Ticker, Trades)
# ==========================================

def get_kline(symbol, interval, limit=30):
    data = _request("GET", f"/kline/get/{symbol}", {"interval": interval, "limit": limit})
    return data.get("data", []) if data else []

def get_tickers():
    return _request("GET", "/ticker/get") or []

def get_orderbook(symbol):
    return _request("GET", f"/market/orderbook/{symbol}") or {"bids": [], "asks": []}

def get_recent_trades(symbol, limit=20):
    return _request("GET", f"/market/trades/{symbol}", {"limit": limit}) or []

# ==========================================
# 2. USER & AUTH (Đăng nhập, Số dư)
# ==========================================

def api_login(username):
    """
    Đăng nhập hoặc tạo user mới.
    Backend đã tự xử lý logic: Nếu user tồn tại thì trả về, chưa thì tạo mới.
    Nếu username là 'admin', backend trả về ID 0.
    """
    return _request("POST", "/users/create", json_data={"username": username})

def api_get_balance(user_id):
    return _request("GET", f"/users/{user_id}")

# ==========================================
# 3. TRADING (Đặt lệnh)
# ==========================================

def api_place_order(user_id, symbol, side, price, amount):
    """
    side: 'buy' hoặc 'sell'
    Trả về tuple: (Success: bool, Message/Data) để frontend dễ xử lý
    """
    payload = {
        "user_id": str(user_id),
        "symbol": str(symbol),  
        "side": str(side),
        "price": float(price),
        "amount": float(amount)
    }
    result = _request("POST", "/orders", json_data=payload)
    
    if result and ("message" in result or "order_id" in result):
        return True, result
    
    # Xử lý lỗi
    detail = result.get("detail", "Lỗi không xác định") if result else "Lỗi kết nối"
    return False, detail

# ==========================================
# 4. ADMIN FEATURES (Quản lý User)
# ==========================================

def api_get_all_users():
    """Lấy danh sách toàn bộ user (cho Admin Panel)"""
    return _request("GET", "/user/get_all") or []

def api_delete_user(user_id):
    """Xóa user theo ID"""
    result = _request("DELETE", f"/user/delete/{user_id}")
    if result and result.get("success"):
        return True, result.get("message")
    
    error_msg = result.get("detail", "Lỗi xóa user") if result else "Lỗi kết nối"
    return False, error_msg