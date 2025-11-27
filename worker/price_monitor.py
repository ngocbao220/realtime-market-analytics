import time
import json
import redis
import requests
import logging
import os

# --- CONFIG ---
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
API_URL = os.getenv("API_URL", "http://api:8000")
CHECK_INTERVAL = 30 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("PriceMonitor")

try:
    r = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
except Exception as e:
    logger.error(f"Failed to connect to Redis: {e}")

def monitor_loop():
    logger.info("🚀 Starting Price Monitor (Rate Limit Fix)...")
    watchlist = ["BTC", "ETH", "BNB", "SOL"]
    last_analysis_time = {}

    while True:
        for symbol in watchlist:
            try:
                # 1. Lấy giá
                ticker_key = f"ticker_1d:{symbol}USDT"
                ticker_data = r.get(ticker_key)
                if not ticker_data: continue

                data = json.loads(ticker_data)
                current_price = float(data.get('Close_price', 0))
                open_price = float(data.get('Open_price', 0))

                if open_price == 0: continue

                # 2. Tính toán
                change_percent = ((current_price - open_price) / open_price) * 100
                
                # 3. Logic Trigger
                current_time = time.time()
                last_time = last_analysis_time.get(symbol, 0)
                should_analyze = False
                trigger_reason = ""

                # Biến động mạnh (>= 3%) -> 5 phút/lần
                if abs(change_percent) >= 3.0:
                    if current_time - last_time > 300: 
                        should_analyze = True
                        trend = "PUMP 🟢" if change_percent > 0 else "DUMP 🔴"
                        trigger_reason = f"🚨 ALERT {trend} (>3%)"
                # Ổn định -> 15 phút/lần
                else:
                    if current_time - last_time > 900: 
                        should_analyze = True
                        trigger_reason = "📉 SIDEWAY UPDATE (<3%)"

                # 4. Gọi API (Có Delay để tránh lỗi Rate Limit)
                if should_analyze:
                    logger.info(f"{trigger_reason}: {symbol} {change_percent:.2f}% | Sending to AI...")
                    
                    payload = {
                        "symbol": symbol,
                        "change_percent": round(change_percent, 2),
                        "current_price": current_price
                    }
                    
                    try:
                        response = requests.post(f"{API_URL}/narrative/analyze", json=payload, timeout=60)
                        
                        if response.status_code == 200:
                            analysis = response.json()
                            logger.info(f"✅ AI Response: {analysis.get('summary')}")
                            last_analysis_time[symbol] = current_time
                            
                            # [QUAN TRỌNG] Nghỉ 10 giây sau mỗi lần gọi thành công 
                            # để Gemini hồi phục quota, tránh lỗi cho đồng coin tiếp theo
                            logger.info("⏳ Cooling down AI for 10s...")
                            time.sleep(5) 
                            
                        else:
                            logger.error(f"API Error ({response.status_code}): {response.text}")
                            
                    except Exception as req_err:
                        logger.error(f"Failed to call API: {req_err}")
                
            except Exception as e:
                logger.error(f"Error checking {symbol}: {e}")
        
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    logger.info("⏳ Waiting 30s for Producer to warm up data...")
    time.sleep(30)
    monitor_loop()