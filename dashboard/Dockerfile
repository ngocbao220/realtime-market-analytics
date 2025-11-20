FROM python:3.10-slim

# Tạo thư mục làm việc
WORKDIR /app

# Copy file requirements.txt và cài đặt
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ source vào /app
COPY . /app

# Expose cổng mặc định của Streamlit
EXPOSE 8501

# Chạy Streamlit
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]
