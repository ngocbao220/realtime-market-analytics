import React, { useEffect, useState, useRef } from 'react';
import { Bot, User, X, Globe, Menu } from 'lucide-react';
import { api } from '../api/client'; 
import '../styles/Header.css'; 

const API_BASE_URL = import.meta.env.VITE_BASE_URL;

const Header = () => {
  const [tickers, setTickers] = useState([]);
  const ws = useRef(null);
  const [showAIPanel, setShowAIPanel] = useState(false);
  const [aiAlerts, setAiAlerts] = useState([]);

  // 1. WebSocket Ticker
  useEffect(() => {
    const socketUrl = api.getWebSocketUrl('/ws/tickers');
    ws.current = new WebSocket(socketUrl);

    ws.current.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (Array.isArray(data)) setTickers(data.slice(0, 4));
      } catch (err) {}
    };
    return () => { if (ws.current) ws.current.close(); };
  }, []);

  // 2. Fetch AI Alerts
  useEffect(() => {
    let interval;
    if (showAIPanel) {
        const fetchAIAlerts = async () => {
            try {
                const res = await fetch(`${API_BASE_URL}/narrative/alerts`);
                if (!res.ok) throw new Error("API Error");
                const data = await res.json();
                if (Array.isArray(data)) setAiAlerts(data);
                else setAiAlerts([]);
            } catch (e) {
                console.error("AI Error:", e);
                setAiAlerts([]);
            }
        };
        fetchAIAlerts();
        interval = setInterval(fetchAIAlerts, 5000); 
    }
    return () => clearInterval(interval);
  }, [showAIPanel]);

  return (
    <header className="header-container">
      {/* --- LEFT: LOGO & MENU --- */}
      <div className="header-left">
        <div className="brand-logo">BINANCE</div>
        <nav className="nav-menu">
          <a href="#" className="nav-item active">Markets</a>
          <a href="#" className="nav-item">Trade</a>
          <a href="#" className="nav-item">Futures</a>
        </nav>
        
        {/* Ticker chạy ngang */}
        <div className="ticker-bar">
          {tickers.map((coin) => {
             const change = parseFloat(coin.change || 0);
             const isPositive = change >= 0;
             return (
               <div key={coin.symbol} className="ticker-item">
                  <span className="t-symbol">{coin.symbol}</span>
                  <span className="t-price">{coin.price}</span>
                  <span className={`t-change ${isPositive ? 'up' : 'down'}`}>
                    {isPositive ? '+' : ''}{change}%
                  </span>
               </div>
             );
          })}
        </div>
      </div>

      {/* --- RIGHT: USER & AI --- */}
      <div className="header-right">
        
        {/* KHU VỰC AI HELPER (Wrapper giữ vị trí) */}
        <div className="ai-wrapper">
            <button 
                className={`ai-btn ${showAIPanel ? 'active' : ''}`}
                onClick={() => setShowAIPanel(!showAIPanel)}
            >
                <Bot size={18} />
                <span>AI Helper</span>
            </button>

            {/* POPUP NỔI (Absolute Position) */}
            {showAIPanel && (
                <div className="ai-popup">
                    <div className="ai-popup-header">
                        <div className="ai-title">
                            <Bot size={18} className="ai-icon-gold" />
                            <span>AI Market Insights</span>
                        </div>
                        <button className="close-btn" onClick={() => setShowAIPanel(false)}>
                            <X size={18} />
                        </button>
                    </div>

                    <div className="ai-popup-body custom-scrollbar">
                        {aiAlerts.length === 0 ? (
                            <div className="empty-state">
                                <div className="pulse-ring"></div>
                                <p>Đang phân tích thị trường...</p>
                            </div>
                        ) : (
                            aiAlerts.map((alert, idx) => {
                                const isPump = alert.change >= 0;
                                const trendClass = isPump ? "trend-up" : "trend-down";
                                
                                return (
                                    <div key={idx} className="ai-card">
                                        <div className="card-header">
                                            <div className="coin-info">
                                                <span className="coin-name">{alert.symbol}</span>
                                                <span className={`coin-change ${trendClass}`}>
                                                    {isPump ? "+" : ""}{alert.change}%
                                                </span>
                                            </div>
                                            <span className="time-stamp">{alert.timestamp}</span>
                                        </div>
                                        
                                        <div className="card-content">
                                            <p>
                                                {alert.analysis?.summary 
                                                    ? alert.analysis.summary.replace(/\$/g, "") 
                                                    : "Đang cập nhật..."}
                                            </p>
                                        </div>
                                    </div>
                                );
                            })
                        )}
                    </div>
                    <div className="ai-popup-footer">
                        Powered by <strong>Gemini 2.5 Flash</strong> & GraphRAG
                    </div>
                </div>
            )}
        </div>

        <div className="icon-btn"><Globe size={18} /></div>
        <div className="icon-btn"><User size={18} /></div>
      </div>
    </header>
  );
};

export default Header;