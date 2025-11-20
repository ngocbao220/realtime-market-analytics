FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    librdkafka-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements và cài vào system
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    echo "✅ Installed packages:" && \
    pip list

WORKDIR /app

CMD ["python", "producer.py"]