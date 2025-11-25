import os

# Lấy URL từ biến môi trường (do docker-compose truyền vào)
# Nếu chạy Docker, nó sẽ lấy giá trị "http://api:8000"
# Nếu chạy Local (tay), nó sẽ lấy "http://127.0.0.1:8000"
API_BASE_URL = os.getenv("API_URL", "http://127.0.0.1:8000")

print(f"🚀 Dashboard is connecting to API at: {API_BASE_URL}")