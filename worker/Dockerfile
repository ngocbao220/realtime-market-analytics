FROM python:3.9-slim

WORKDIR /app

# Cài thư viện redis
RUN pip install redis requests

COPY . .

CMD ["python", "-u", "main.py"]