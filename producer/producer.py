import asyncio
from websocket_orderbook import orderbook_stream
from websocket_ticker import ticker_stream
from websocket_trade import trade_stream
from websocket_kline import kline_stream

if __name__ == "__main__":
    symbols = ["btcusdt", "ethusdt", "solusdt", "bnbusdt", "dogeusdt"]  # Danh sách symbol cần subscribe
    interval_ticker = "1d"
    interval_kline = "1m"

    loop = asyncio.get_event_loop()
    tasks = []

    for s in symbols:
        tasks.append(trade_stream(s))
        tasks.append(ticker_stream(s, interval_ticker))
        tasks.append(orderbook_stream(s))
        tasks.append(kline_stream(s, interval_kline))

    loop.run_until_complete(asyncio.gather(*tasks))
