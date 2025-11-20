# Financial Risk Dashboard - Quick Start

## 🚀 Start Full Stack

```bash
# 1. Start all services
sudo docker compose up -d

# 2. Verify services
sudo docker compose ps

# 3. Check logs
sudo docker compose logs -f spark-submit
```

## 📊 Access Points

- **ClickHouse Web UI**: http://localhost:8123/play
- **API Documentation**: http://localhost:8000/docs
- **Kafka UI**: http://localhost:8085
- **Spark Master UI**: http://localhost:8080

## 🎨 Run Dashboard

```bash
cd dashboard
pip install -r requirements.txt
streamlit run app.py
```

Dashboard: http://localhost:8501

## 📡 API Endpoints

```bash
# Health check
curl http://localhost:8000/

# Get symbols
curl http://localhost:8000/api/symbols

# Trading stats
curl http://localhost:8000/api/stats/BTCUSDT?hours=24

# Price history
curl http://localhost:8000/api/price-history/BTCUSDT?interval=1m

# Realtime trades
curl http://localhost:8000/api/realtime/BTCUSDT?limit=10

# Orderbook
curl http://localhost:8000/api/orderbook/BTCUSDT
```

## 🔍 Query ClickHouse Directly

```bash
# Via CLI
sudo docker compose exec clickhouse clickhouse-client -u default --password 12345

# Via Web UI
http://localhost:8123/play
```

Example queries:
```sql
-- Count trades
SELECT COUNT(*) FROM default.trades;

-- Trading volume by symbol
SELECT Symbol, SUM(TradeValue) FROM default.trades GROUP BY Symbol;

-- Recent trades
SELECT * FROM default.trades ORDER BY TradeTime DESC LIMIT 10;
```

## 🛑 Stop Services

```bash
sudo docker compose down
```

## 📦 Architecture

```
Binance → Producer → Kafka → Spark → ClickHouse → API → Dashboard
                                  ↓
                              Parquet (optional backup)
```
