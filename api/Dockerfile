# Sử dụng Python 3.10 Slim để nhẹ và ổn định
FROM python:3.10-slim

# Thiết lập thư mục làm việc là /app
WORKDIR /app

# Copy file danh sách thư viện vào trước
COPY requirements.txt .

# Cài đặt các thư viện
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ mã nguồn dự án vào container
COPY . .

# Mở cổng cho Backend (8001) và Frontend (8502)
EXPOSE 8001
EXPOSE 8502

# Lệnh mặc định khi chạy container (sẽ bị docker-compose ghi đè)
# Chạy uvicorn trỏ vào module "backend.main"
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]