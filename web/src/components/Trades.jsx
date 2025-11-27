import React, { useEffect, useState, useRef } from 'react';
import '../styles/Trades.css';
import { api } from '../api/client'; // Import API client

const Trades = ({ symbol = "BTCUSDT" }) => {
  const [trades, setTrades] = useState([]);
  const wsRef = useRef(null);

  useEffect(() => {
    // Cập nhật dùng api.getWebSocketUrl
    const endpoint = `/market/ws/trades/${symbol}?type=real&mode=real_time&limit=50`;
    const socketUrl = api.getWebSocketUrl(endpoint);
    
    const ws = new WebSocket(socketUrl);
    wsRef.current = ws;

    ws.onopen = () => {
        console.log(`✅ Connected to Trades WS: ${symbol}`);
    };

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            
            if (Array.isArray(data)) {
                setTrades(data); 
            }
        } catch (err) {
            console.error("Error parsing Trades WS message:", err);
        }
    };

    ws.onerror = (error) => {
        console.error("WebSocket Trades Error:", error);
    };

    return () => {
        if (wsRef.current) {
            wsRef.current.close();
            console.log(`Closed Trades WS: ${symbol}`);
        }
    };
  }, [symbol]);

  // --- FORMATTERS ---
  const formatPrice = (price) => {
    return new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(price);
  };

  const formatAmount = (num) => {
    return parseFloat(num).toFixed(5);
  };

  const formatTime = (timeData) => {
    return timeData; 
  };

  return (
    <div className="trades-container">
      <div className="trades-header">
        Market Trades
      </div>

      <div className="trades-thead">
        <span className="tr-col col-price">Price(USDT)</span>
        <span className="tr-col col-amount">Amount({symbol.replace("USDT", "")})</span>
        <span className="tr-col col-time">Time</span>
      </div>

      <div className="trades-list">
        {trades.length === 0 && <div className="text-center py-4 opacity-50">Waiting for data...</div>}
        
        {trades.map((trade, index) => {
           const isBuy = trade.side === 'BUY'; 
           const colorClass = isBuy ? 'text-green' : 'text-red';
           
           return (
             <div key={index} className="trade-row">
               <span className={`tr-col col-price ${colorClass}`}>
                  {formatPrice(trade.price)}
               </span>
               <span className={`tr-col col-amount text-white`}>
                  {formatAmount(trade.amount)}
               </span>
               <span className={`tr-col col-time text-right`}>
                  {formatTime(trade.time)}
               </span>
             </div>
           );
        })}
      </div>
    </div>
  );
};

export default Trades;