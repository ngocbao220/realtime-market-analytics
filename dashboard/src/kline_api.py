import requests
from config import API_BASE_URL

def api_get_kline(symbol, interval="1m"):
    """
    Lấy dữ liệu nến (OHLCV) từ backend cho symbol và interval.
    Trả về list dict: [{timestamp, open, high, low, close, volume}]
    """
    url = f"{API_BASE_URL}/api/kline/{symbol}?interval={interval}"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            return response.json().get("data", [])
        else:
            # Lấy thông báo lỗi từ backend nếu có
            detail = response.json().get("detail", "Lỗi không xác định")
            return []
    except Exception as e:
        return []
