## Project structure
``` bash
project/
│
├── kafka/          → cấu hình Kafka topic
├── spark/          → Spark job (streaming từ Kafka sang Cassandra)
├── cassandra/      → storage
├── api/            → FastAPI backend
├── dashboard/      → React web app
└── docker-compose.yml
```

## Quick test

```bash
    git clone https://github.com/ngocbao220/Financial_Risk.git
    pip install requirements.txt
    # Load `data source` to folder data
    python prepare_data.py
```
