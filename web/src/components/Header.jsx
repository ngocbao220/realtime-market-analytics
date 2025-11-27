import React, { useEffect, useState, useRef } from 'react';
import { Bot, User } from 'lucide-react'; 
import { api } from '../api/client'; // 1. Import api instance
import '../styles/Header.css'; 

const Header = () => {
  const [tickers, setTickers] = useState([]);
  const ws = useRef(null);

  useEffect(() => {
    // 2. Sử dụng hàm helper để lấy URL động (Tự động đổi ws://localhost hoặc ws://domain.com)
    const socketUrl = api.getWebSocketUrl('/ws/tickers');
    
    // Debug để bạn yên tâm là nó đang kết nối đúng đâu
    console.log("Connecting Ticker WS to:", socketUrl); 

    ws.current = new WebSocket(socketUrl);

    ws.current.onopen = () => {
      console.log("✅ Connected to Ticker WebSocket");
    };

    ws.current.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (Array.isArray(data)) {
           setTickers(data.slice(0, 4));
        }
      } catch (err) {
        console.error("Lỗi parse data ticker:", err);
      }
    };

    ws.current.onerror = (error) => {
      console.error("WebSocket Error:", error);
    };

    return () => {
      if (ws.current) {
        ws.current.close();
      }
    };
  }, []);

  return (
    <header className="header-container">
      <div className="left-section">
        <div className="brand-logo">BINANCE</div>
        
        <nav className="nav-menu">
          <a href="#" className="nav-link">Markets</a>
          <a href="#" className="nav-link">Trade</a>
        </nav>

        <div className="ticker-section">
          {tickers.map((coin) => {
             const change = parseFloat(coin.change || 0);
             const isPositive = change >= 0;
             const colorClass = isPositive ? 'text-green' : 'text-red';
             
             return (
               <div key={coin.symbol} className="ticker-item">
                  <span className="ticker-symbol">{coin.symbol}</span>
                  <div className="flex gap-2">
                      <span className="ticker-price">{coin.price}</span>
                      <span className={`text-xs ${colorClass}`}>
                        {isPositive ? '+' : ''}{change}%
                      </span>
                  </div>
               </div>
             );
          })}
          
          {tickers.length === 0 && (
            <span className="ticker-loading">Connecting WS...</span>
          )}
        </div>
      </div>

      <div className="right-section">
        <button className="ai-btn">
          <Bot size={18} />
          <span>AI Helper</span>
        </button>
        <div className="user-avatar">
            <User size={16} />
        </div>
      </div>
    </header>
  );
};

export default Header;