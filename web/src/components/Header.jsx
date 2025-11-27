import React, { useEffect, useState, useRef } from 'react';
import { Bot, User, X, ArrowLeft, MessageSquare, TrendingUp, Newspaper } from 'lucide-react'; 
import { api } from '../api/client'; 
import '../styles/Header.css'; 

const API_BASE_URL = import.meta.env.VITE_BASE_URL;

const Header = () => {
  const [tickers, setTickers] = useState([]);
  const ws = useRef(null);
  
  // State quản lý AI Panel
  const [showAIPanel, setShowAIPanel] = useState(false);
  const [aiView, setAiView] = useState('menu'); 
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
    if (showAIPanel && aiView === 'analysis') {
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
  }, [showAIPanel, aiView]);

  const handleTogglePanel = () => {
      console.log("Toggle AI Panel:", !showAIPanel); // Debug check
      setShowAIPanel(!showAIPanel);
  };

  const handleClosePanel = () => {
      setShowAIPanel(false);
      setTimeout(() => setAiView('menu'), 200);
  };

  return (
    <header className="header-container">
      {/* --- LEFT SECTION --- */}
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
                      <span className={`text-xs ${colorClass}`}>{isPositive ? '+' : ''}{change}%</span>
                  </div>
               </div>
             );
          })}
        </div>
      </div>

      {/* --- RIGHT SECTION --- */}
      <div className="right-section">
        <div className="ai-wrapper">
            {/* Nút mở Chatbot */}
            <button className={`ai-btn ${showAIPanel ? 'active' : ''}`} onClick={handleTogglePanel}>
                <Bot size={18} />
                <span>AI Helper</span>
            </button>

            {/* KHUNG CHAT BOT */}
            {showAIPanel && (
                <div className="ai-popup">
                    <div className="ai-popup-header">
                        <div className="ai-header-left">
                            {aiView !== 'menu' ? (
                                <button className="back-btn" onClick={() => setAiView('menu')}>
                                    <ArrowLeft size={18} />
                                </button>
                            ) : (
                                <Bot size={18} className="ai-icon-gold" />
                            )}
                            <span className="ai-title">Chat Assistant</span>
                        </div>
                        <button className="close-btn" onClick={handleClosePanel}>
                            <X size={18} />
                        </button>
                    </div>

                    <div className="ai-popup-body custom-scrollbar">
                        
                        {/* VIEW 1: CHAT MENU */}
                        {aiView === 'menu' && (
                            <div className="ai-chat-container">
                                <div className="chat-bubble bot">
                                    Xin chào! Tôi là trợ lý AI. Bạn muốn xem thông tin gì hôm nay?
                                </div>
                                <div className="chat-options">
                                    <button className="option-btn" onClick={() => setAiView('summary')}>
                                        <Newspaper size={16} className="text-[#F0B90B]" />
                                        <span>Tổng hợp tin tức 1 tuần qua</span>
                                    </button>
                                    <button className="option-btn" onClick={() => setAiView('analysis')}>
                                        <TrendingUp size={16} className="text-[#F0B90B]" />
                                        <span>Dự báo biến động giá 24h</span>
                                    </button>
                                </div>
                            </div>
                        )}

                        {/* VIEW 2: PHÂN TÍCH GIÁ */}
                        {aiView === 'analysis' && (
                            <div className="ai-content-view">
                                <div className="chat-bubble bot mb-3">
                                    Dưới đây là các phân tích biến động giá mới nhất:
                                </div>
                                {aiAlerts.length === 0 ? (
                                    <div className="loading-state">
                                        <div className="typing-indicator">
                                            <span></span><span></span><span></span>
                                        </div>
                                        <p>Đang phân tích dữ liệu...</p>
                                    </div>
                                ) : (
                                    aiAlerts.map((alert, idx) => {
                                        const isPump = alert.change >= 0;
                                        const trendClass = isPump ? "trend-up" : "trend-down";
                                        const colorClass = isPump ? "text-[#0ECB81]" : "text-[#F6465D]";
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
                                                        <span className={`${colorClass} font-bold mr-1`}>AI:</span>
                                                        {alert.analysis?.summary || "Đang cập nhật..."}
                                                    </p>
                                                </div>
                                            </div>
                                        );
                                    })
                                )}
                            </div>
                        )}

                        {/* VIEW 3: TIN TỨC */}
                        {aiView === 'summary' && (
                            <div className="ai-content-view">
                                <div className="chat-bubble bot mb-3">
                                    Đang tổng hợp dữ liệu tin tức tuần qua...
                                </div>
                                <div className="empty-state">
                                    <p className="text-[#EAECEF] font-bold">Tính năng đang phát triển 🚀</p>
                                    <p className="text-[#848E9C]">AI sẽ sớm cung cấp báo cáo tại đây.</p>
                                </div>
                            </div>
                        )}
                    </div>

                    <div className="ai-popup-input-area">
                        <input type="text" placeholder="Hỏi AI điều gì đó..." disabled />
                        <button disabled><MessageSquare size={16} /></button>
                    </div>
                </div>
            )}
        </div>
        <div className="user-avatar"><User size={16} /></div>
      </div>
    </header>
  );
};

export default Header;