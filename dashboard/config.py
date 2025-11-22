import os

# --- CẤU HÌNH KẾT NỐI ---
API_PORT = os.getenv("API_PORT", "8000")
API_BASE_URL = f"http://api:{API_PORT}" 