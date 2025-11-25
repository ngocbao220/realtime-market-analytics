# Sử dụng Python 3.10 Slim
FROM python:3.10-slim

# Thiết lập thư mục làm việc
WORKDIR /app

# Copy requirements và cài đặt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code vào container
COPY . .

# Expose cổng 8000
EXPOSE 8000

# [QUAN TRỌNG] Sửa lệnh chạy: main:app (vì main.py nằm ngay tại /app)
# Port 8000 để khớp với config
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]