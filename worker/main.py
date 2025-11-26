import time
import logging

from logic import process_symbol

logging.basicConfig(level=logging.INFO, format="%(asctime)s - ENGINE - %(message)s")

def run_engine():
    logging.info("🚀 Matching Engine Started...")
    symbols = ["BTCUSDT"] # List các cặp tiền cần khớp BNBUSDT, .....
    # Chỉ demo BTC
    
    while True:
        try:
            for symbol in symbols:
                process_symbol(symbol)
                
            time.sleep(0.05) # Sleep ngắn để đỡ tốn CPU
            
        except Exception as e:
            time.sleep(1)

if __name__ == "__main__":
    run_engine()