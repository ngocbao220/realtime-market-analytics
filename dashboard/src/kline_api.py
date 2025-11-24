# File: src/kline_api.py
import requests
from config import API_BASE_URL

def api_get_kline(symbol, interval="1m"):
    """
    Gọi API nội bộ (FastAPI) để lấy dữ liệu từ Redis
    """
    # Endpoint khớp với backend.py: /api/kline/{symbol}
    url = f"{API_BASE_URL}/api/kline/{symbol}"
    params = {"interval": interval}
    
    try:
        # Gọi vào localhost:8000
        response = requests.get(url, params=params, timeout=2)
        if response.status_code == 200:
            return response.json().get("data", [])
        else:
            print(f"API Error: {response.status_code}")
            return []
    except Exception as e:
        print(f"Connection Error: {e}")
        return []