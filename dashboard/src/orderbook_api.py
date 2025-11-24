import requests
from config import API_BASE_URL

# --- HÀM CŨ (GIỮ NGUYÊN) ---
def api_place_order(user_id, side, price, amount):
    url = f"{API_BASE_URL}/orders/{side}"
    payload = {
        "user_id": str(user_id),
        "price": float(price),
        "amount": float(amount)
    }
    try:
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code == 200:
            return True, response.json()
        else:
            detail = response.json().get("detail", "Lỗi không xác định")
            return False, detail
    except Exception as e:
        return False, str(e)

# --- HÀM MỚI: LẤY ORDERBOOK ---
def api_get_orderbook(symbol):
    """
    Lấy danh sách Bids (Mua) và Asks (Bán) từ API Backend
    Trả về dict: {"bids": [[price, qty], ...], "asks": [[price, qty], ...]}
    """
    # Backend đã viết sẵn endpoint này
    url = f"{API_BASE_URL}/api/orderbook/{symbol}"
    try:
        response = requests.get(url, timeout=3)
        if response.status_code == 200:
            return response.json()
        return {"bids": [], "asks": []}
    except:
        return {"bids": [], "asks": []}