import os

# --- CẤU HÌNH KẾT NỐI ---
API_PORT = os.getenv("API_PORT", "8001")
API_BASE_URL = f"http://127.0.0.1:{API_PORT}" 