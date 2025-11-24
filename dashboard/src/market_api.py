import requests
from config import API_BASE_URL

def api_get_tickers():
    """Lấy danh sách giá các đồng coin"""
    url = f"{API_BASE_URL}/api/market/tickers"
    try:
        response = requests.get(url, timeout=2)
        if response.status_code == 200:
            return response.json()
        return []
    except:
        return []

def api_get_recent_trades(symbol):
    """Lấy lịch sử giao dịch gần nhất của 1 symbol"""
    url = f"{API_BASE_URL}/api/trades/{symbol}"
    try:
        response = requests.get(url, timeout=2)
        if response.status_code == 200:
            return response.json()
        return []
    except:
        return []