import React, { createContext, useContext, useEffect, useState, useRef } from 'react';
import { api } from '../api/client';

const TickerContext = createContext(null);

export const TickerProvider = ({ children }) => {
  // Lưu trữ dưới dạng Object: { "BTCUSDT": { price: 90000, ... }, "ETHUSDT": { ... } }
  const [tickers, setTickers] = useState({});
  const ws = useRef(null);

  useEffect(() => {
    const socketUrl = api.getWebSocketUrl('/ws/tickers');
    console.log("🔌 TickerProvider: Connecting WS...");

    ws.current = new WebSocket(socketUrl);

    ws.current.onopen = () => {
      console.log("✅ TickerProvider: Connected");
    };

    ws.current.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (Array.isArray(data)) {
          // Chuyển đổi Array thành Object để truy xuất nhanh theo Symbol key
          // Ví dụ: tickers['BTCUSDT'] sẽ nhanh hơn data.find(...)
          const tickerMap = {};
          data.forEach(item => {
            tickerMap[item.symbol] = item;
          });
          setTickers(tickerMap);
        }
      } catch (err) {
        console.error("TickerProvider Parse Error:", err);
      }
    };

    return () => {
      if (ws.current) ws.current.close();
    };
  }, []);

  return (
    <TickerContext.Provider value={tickers}>
      {children}
    </TickerContext.Provider>
  );
};

// Custom Hook để lấy toàn bộ danh sách (cho Header)
export const useAllTickers = () => {
  return useContext(TickerContext);
};

// Custom Hook để lấy thông tin chi tiết 1 cặp coin (cho SymbolInfo, Orderbook)
export const useSymbolTicker = (symbol) => {
  const tickers = useContext(TickerContext);
  return tickers[symbol] || null;
};