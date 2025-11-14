import requests
import pandas as pd
import time

def fetch_binance(symbol, interval, start_ms, end_ms):
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 1000
    }
    
    data = requests.get(url, params=params).json()
    return data

def fetch_full_binance(symbol, interval="1m", start_year=2023, end_year=2024):
    start_ts = int(time.mktime(time.strptime(f"{start_year}-01-01", "%Y-%m-%d")) * 1000)
    end_ts   = int(time.mktime(time.strptime(f"{end_year}-01-01", "%Y-%m-%d")) * 1000)

    all_rows = []
    current = start_ts

    print("⏳ Bắt đầu crawl Binance...")

    while current < end_ts:
        data = fetch_binance(symbol, interval, current, end_ts)

        if not data or "code" in data:
            print("⚠ Binance limit, đợi 1 giây...")
            time.sleep(1)
            continue

        all_rows.extend(data)

        # next timestamp = close time của nến cuối + 1 ms
        current = data[-1][6] + 1

        print("📌 Lấy tới:", pd.to_datetime(current, unit='ms'))

        time.sleep(0.3)  # tránh rate limit

    print("✅ Hoàn thành crawl!")

    # Convert sang DataFrame
    df = pd.DataFrame(all_rows, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")

    return df

# RUN
df = fetch_full_binance(symbol="BTCUSDT", interval="1m", start_year=2023, end_year=2024)
print(df.head())

df.to_csv("btc_1m_2023.csv", index=False)
df = fetch_full_binance(symbol="ETHUSDT", interval="1m", start_year=2023, end_year=2024)
print(df.head())

df.to_csv("eth_1m_2023.csv", index=False)
print("💾 Saved to eth_1m_2023.csv")
