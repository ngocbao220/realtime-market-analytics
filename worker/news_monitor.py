import time
import requests
import logging
import os

# --- CONFIG ---
# Chỉ cần gọi vào API, không cần logic phức tạp
API_URL = os.getenv("API_URL", "http://34.124.203.62:8000")
# Thời gian cập nhật: 4 Tiếng/lần (4 * 60 * 60 = 14400s)
CHECK_INTERVAL = 14400 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("NewsMonitor")

def news_monitor_loop():
    logger.info("📰 Starting Weekly News Summarizer Worker...")
    
    # Chạy ngay lần đầu tiên khi khởi động
    first_run = True

    while True:
        try:
            if first_run:
                logger.info("🚀 First run: Triggering AI summary immediately...")
            else:
                logger.info("⏰ Routine check: Triggering AI summary...")

            # Gọi API để Backend tự xử lý việc Query DB -> Gọi Gemini -> Lưu Redis
            response = requests.post(f"{API_URL}/narrative/summarize-news", timeout=60)

            if response.status_code == 200:
                logger.info("✅ Triggered successfully. AI is processing in background.")
            else:
                logger.error(f"❌ Failed to trigger API. Status: {response.status_code}")

        except Exception as e:
            logger.error(f"❌ Connection Error: {e}")

        # Tắt cờ first run
        first_run = False
        
        # Ngủ 4 tiếng
        logger.info(f"💤 Sleeping for {CHECK_INTERVAL}s (4 hours)...")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    # Đợi 1 chút để API server khởi động xong nếu chạy cùng lúc
    time.sleep(15) 
    news_monitor_loop()