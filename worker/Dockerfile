FROM python:3.9-slim

WORKDIR /app

# Cài thư viện redis
RUN pip install redis

COPY engine.py .

CMD ["python", "-u", "engine.py"]