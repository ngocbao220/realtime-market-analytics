import React, { useEffect, useState } from 'react';
import { Bot, User } from 'lucide-react'; 
import { api } from '../api/client'; 
// Import file CSS vừa tạo
import '../styles/Header.css'; 

const Header = () => {
  const [tickers, setTickers] = useState([]);

  useEffect(() => {
    const fetchTickers = async () => {
      try {
        const data = await api.getTickers(); 
        if (Array.isArray(data)) {
           setTickers(data.slice(0, 4));
        }
      } catch (e) { console.log("Chưa kết nối API"); }
    };
    fetchTickers();
  }, []);

  return (
    <header className="header-container">
      {/* Logo & Nav Wrapper */}
      <div className="left-section">
        <div className="brand-logo">BINANCE</div>
        
        {/* Menu Desktop */}
        <nav className="nav-menu">
          <a href="#" className="nav-link">Markets</a>
          <a href="#" className="nav-link">Trade</a>
        </nav>

        {/* Ticker chạy giá */}
        <div className="ticker-section">
          {tickers.map((coin) => (
             <div key={coin.symbol} className="ticker-item">
                <span className="ticker-symbol">{coin.symbol}</span>
                <span className="ticker-price">{coin.price}</span>
             </div>
          ))}
          {tickers.length === 0 && <span className="ticker-loading">Loading Tickers...</span>}
        </div>
      </div>

      {/* Nút AI & User */}
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
