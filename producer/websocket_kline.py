
import asyncio
import websockets
import json
from helpers import convert_time
from kafka_producer import send_to_kafka

async def kline_stream(symbol: str, interval: str = "1m"):
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@kline_{interval}"

    async with websockets.connect(url) as ws:
        print(f"[KLINE] Connected to {symbol} kline_{interval}")
        while True:
            try:
                msg = await ws.recv()
                data = json.loads(msg)

                kline = data.get('k', {})
                is_candle_closed = kline.get('x', False)

                event_time = convert_time(data.get('E'))
                open_time = convert_time(kline.get('t'))
                close_time = convert_time(kline.get('T'))
                symbol_k = kline.get('s')
                interval_k = kline.get('i')
                first_trade_id = kline.get('f')
                last_trade_id = kline.get('L')
                open_price = kline.get('o')
                high_price = kline.get('h')
                low_price = kline.get('l')
                close_price = kline.get('c')
                volume = kline.get('v')
                trades = kline.get('n')
                is_closed = kline.get('x')
                quote_volume = kline.get('q')
                taker_buy_base_vol = kline.get('V')
                taker_buy_quote_vol = kline.get('Q')

                status = "CLOSED" if is_candle_closed else "OPEN"
                print(f"\n===== {symbol.upper()} {interval} KLINE {status} =====")
                print(f"Event time:              {event_time}")
                print(f"Open time:               {open_time}")
                print(f"Close time:              {close_time}")
                print(f"Symbol:                  {symbol_k}")
                print(f"Interval:                {interval_k}")
                print(f"First trade ID:          {first_trade_id}")
                print(f"Last trade ID:           {last_trade_id}")
                print(f"Open price:              {open_price}")
                print(f"High price:              {high_price}")
                print(f"Low price:               {low_price}")
                print(f"Close price:             {close_price}")
                print(f"Volume:                  {volume}")
                print(f"Number of trades:        {trades}")
                print(f"Is closed:               {is_closed}")
                print(f"Quote volume:            {quote_volume}")
                print(f"Taker buy base volume:   {taker_buy_base_vol}")
                print(f"Taker buy quote volume:  {taker_buy_quote_vol}")
                print("======================================\n")

                send_to_kafka(f'binance_kline_{interval}', data)

            except Exception as e:
                print(f"[KLINE] Error: {e}")
                await asyncio.sleep(1)

if __name__ == "__main__":
    symbols = ["btcusdt", "bnbbtc"]
    interval = "1m"
    loop = asyncio.get_event_loop()
    tasks = [kline_stream(s, interval) for s in symbols]
    loop.run_until_complete(asyncio.gather(*tasks))