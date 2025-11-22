import requests
from config import API_BASE_URL

def api_place_order(user_id, side, price, amount):
    """
    side: 'buy' hoặc 'sell'
    """
    # Endpoint: /orders/buy hoặc /orders/sell
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
            # Lấy thông báo lỗi từ backend
            detail = response.json().get("detail", "Lỗi không xác định")
            return False, detail
    except Exception as e:
        return False, str(e)