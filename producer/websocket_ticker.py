import asyncio
import websockets
import json
from kafka_producer import send_to_kafka
from helpers import convert_time

async def ticker_stream(symbol: str, interval: str = "1d"):
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@ticker_{interval}"

    async with websockets.connect(url) as ws:   
        print(f"[TICKER] Connected to {symbol} ticker_{interval}")
        while True:
            try:
                msg = await ws.recv()
                data = json.loads(msg)

                # Chuyển timestamp sang readable
                event_time = convert_time(data.get('E'))
                open_time = convert_time(data.get('O'))
                close_time = convert_time(data.get('C'))

                # Lấy các trường quan trọng
                symbol = data.get('s')
                price_open = data.get('o')
                price_high = data.get('h')
                price_low = data.get('l')
                price_close = data.get('c')
                volume_base = data.get('v')
                volume_quote = data.get('q')

                # Log đẹp trên terminal
                print(f"\n===== {symbol} {interval} Ticker =====")
                print(f"Event time:            {event_time}")
                print(f"Statistics open time:  {open_time}")
                print(f"Statistics close time: {close_time}")
                print(f"Open price:            {price_open}")
                print(f"High price:            {price_high}")
                print(f"Low price:             {price_low}")
                print(f"Close price:           {price_close}")
                print(f"Base asset volume:     {volume_base}")
                print(f"Quote asset volume:    {volume_quote}")
                print("===============================\n")

                send_to_kafka(f'binance_tickers_{interval}', data)

            except Exception as e:
                print(f"[TICKER] Error: {e}")
                await asyncio.sleep(1)

if __name__ == "__main__":
    symbols = ["btcusdt", "bnbbtc"]
    interval = "1h"
    loop = asyncio.get_event_loop()
    tasks = [ticker_stream(s, interval) for s in symbols]
    loop.run_until_complete(asyncio.gather(*tasks))
