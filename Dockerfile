FROM python:3.10-slim
WORKDIR /app
COPY app/producer.py /app/producer.py
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "/app/producer.py"]
