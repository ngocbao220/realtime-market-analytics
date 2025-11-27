import React, { useEffect, useState, useRef } from 'react';
import { Bot, User, X, Globe, Menu } from 'lucide-react';
import { api } from '../api/client'; 
import '../styles/Header.css'; 

const API_BASE_URL = import.meta.env.VITE_BASE_URL;

const Header = () => {
  const [tickers, setTickers] = useState([]);
  const ws = useRef(null);
  
  // State quản lý AI Panel
  const [showAIPanel, setShowAIPanel] = useState(false);
  const [aiView, setAiView] = useState('menu'); // 'menu', 'summary', 'analysis'
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

  // 2. Fetch AI Alerts (Gọi khi vào view 'analysis')
  useEffect(() => {
    let interval;
    // Chỉ fetch khi panel mở VÀ đang ở tab phân tích giá
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

  // Reset view về menu khi đóng panel
  const handleClosePanel = () => {
      setShowAIPanel(false);
      setTimeout(() => setAiView('menu'), 200); // Delay nhẹ để đóng xong mới reset
  };

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
                      <span className={`text-xs ${colorClass}`}>{isPositive ? '+' : ''}{change}%</span>
                  </div>
               </div>
             );
          })}
        </div>
      </div>

      <div className="right-section">
        {/* AI Helper Button & Popup */}
        <div className="ai-wrapper">
            <button className={`ai-btn ${showAIPanel ? 'active' : ''}`} onClick={() => setShowAIPanel(!showAIPanel)}>
                <Bot size={18} />
                <span>AI Helper</span>
            </button>

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
                            <span className="ai-title">
                                {aiView === 'menu' && 'AI Market Helper'}
                                {aiView === 'summary' && 'Tóm tắt thị trường'}
                                {aiView === 'analysis' && 'Phân tích biến động'}
                            </span>
                        </div>
                        <button className="close-btn" onClick={handleClosePanel}>
                            <X size={18} />
                        </button>
                    </div>

                    <div className="ai-popup-body custom-scrollbar">
                        {/* VIEW 1: MENU CHÍNH */}
                        {aiView === 'menu' && (
                            <div className="ai-menu-options">
                                <button className="ai-menu-item" onClick={() => setAiView('summary')}>
                                    <div className="icon-box"><FileText size={20} /></div>
                                    <div className="menu-text">
                                        <span className="menu-title">Tóm tắt thị trường (7 ngày)</span>
                                        <span className="menu-desc">Tổng hợp sự kiện & xu hướng vĩ mô</span>
                                    </div>
                                </button>
                                <button className="ai-menu-item" onClick={() => setAiView('analysis')}>
                                    <div className="icon-box"><TrendingUp size={20} /></div>
                                    <div className="menu-text">
                                        <span className="menu-title">Biến động giá Crypto</span>
                                        <span className="menu-desc">Phân tích nguyên nhân Tăng/Giảm</span>
                                    </div>
                                </button>
                            </div>
                        )}

                        {/* VIEW 2: PHÂN TÍCH GIÁ (Dữ liệu cũ) */}
                        {aiView === 'analysis' && (
                            <>
                                {aiAlerts.length === 0 ? (
                                    <div className="empty-state">
                                        <BarChart2 size={32} className="mb-2 opacity-50" />
                                        <p>Đang phân tích dữ liệu...</p>
                                    </div>
                                ) : (
                                    aiAlerts.map((alert, idx) => {
                                        const isPump = alert.change >= 0;
                                        const colorClass = isPump ? "text-[#0ECB81]" : "text-[#F6465D]";
                                        const bgClass = isPump ? "bg-[#0ECB81]/10" : "bg-[#F6465D]/10";
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
                                                        <span className={`${colorClass} font-bold mr-1`}>AI:</span>
                                                        {alert.analysis?.summary ? alert.analysis.summary.replace(/\$/g, "") : "Đang cập nhật..."}
                                                    </p>
                                                </div>
                                            </div>
                                        );
                                    })
                                )}
                            </>
                        )}

                        {/* VIEW 3: TÓM TẮT THỊ TRƯỜNG (Placeholder cho tính năng tiếp theo) */}
                        {aiView === 'summary' && (
                            <div className="empty-state">
                                <div className="text-center p-4">
                                    <p className="text-[#EAECEF] font-bold mb-2">Tính năng đang phát triển 🚀</p>
                                    <p className="text-[#848E9C]">AI sẽ sớm cung cấp báo cáo tổng quan thị trường dựa trên 168h dữ liệu lịch sử.</p>
                                </div>
                            </div>
                        )}
                    </div>
                    <div className="ai-popup-footer">
                        Powered by <strong>Gemini 2.5 Flash</strong> & GraphRAG
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