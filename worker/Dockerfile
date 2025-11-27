FROM python:3.9-slim

WORKDIR /app

# Cài đặt thư viện
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code (bao gồm start.sh, main.py, price_monitor.py)
COPY . .

# Cấp quyền thực thi cho script
RUN chmod +x start.sh

# Chạy script khởi động thay vì chạy python trực tiếp
CMD ["./start.sh"]