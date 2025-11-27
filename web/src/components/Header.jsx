import React, { useEffect, useState, useRef } from 'react';
import { User, X, TrendingUp, Newspaper, FileText } from 'lucide-react';
import { api } from '../api/client';
import '../styles/Header.css';

const API_BASE_URL = import.meta.env.VITE_BASE_URL;

const Header = () => {
  const [tickers, setTickers] = useState([]);
  const ws = useRef(null);

  // State quản lý Popup
  const [activePopup, setActivePopup] = useState(null);
  const [aiAlerts, setAiAlerts] = useState([]); // Dữ liệu cho nút Dự báo
  const [newsList, setNewsList] = useState([]); // Dữ liệu cho nút Tin tức
  const [loading, setLoading] = useState(false);

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

  // 2. Fetch Data khi mở Popup
  useEffect(() => {
    let interval;
    const fetchData = async () => {
      setLoading(true);
      try {
        if (activePopup === 'price') {
          // Gọi API lấy Alerts (Dự báo)
          const res = await fetch(`${API_BASE_URL}/narrative/alerts`);
          if (res.ok) {
            const data = await res.json();
            console.log("Fetched AI Alerts:", data);  
            setAiAlerts(Array.isArray(data) ? data : []);
          }
        } else if (activePopup === 'news') {
          // Gọi API lấy News (Tin tức)
          const res = await fetch(`${API_BASE_URL}/narrative/news`);
          if (res.ok) {
            const data = await res.json();
            console.log("Fetched News List:", data);
            setNewsList(Array.isArray(data) ? data : []);
          }
        }
      } catch (e) {
        console.error("API Error:", e);
      } finally {
        setLoading(false);
      }
    };

    if (activePopup) {
      fetchData(); // Fetch ngay khi mở
      interval = setInterval(fetchData, 10000); // Refresh mỗi 10s
    }

    return () => clearInterval(interval);
  }, [activePopup]);

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
        <div className="ai-actions-group">
          
          {/* Nút 1: Tin Tức */}
          <button
            className={`action-btn ${activePopup === 'news' ? 'active' : ''}`}
            onClick={() => setActivePopup(activePopup === 'news' ? null : 'news')}
          >
            <Newspaper size={16} />
            <span>Tin tức</span>
          </button>

          {/* Nút 2: Dự Báo */}
          <button
            className={`action-btn ${activePopup === 'price' ? 'active' : ''}`}
            onClick={() => setActivePopup(activePopup === 'price' ? null : 'price')}
          >
            <TrendingUp size={16} />
            <span>Dự báo</span>
          </button>

          {/* KHUNG POPUP */}
          {activePopup && (
            <div className="ai-popup">
              <div className="ai-popup-header">
                <div className="ai-header-left">
                  {activePopup === 'news' ? (
                    <FileText size={18} className="text-[#F0B90B]" />
                  ) : (
                    <TrendingUp size={18} className="text-[#F0B90B]" />
                  )}
                  <span className="ai-title">
                    {activePopup === 'news' ? 'Tin tức mới nhất (Crypto)' : 'Biến động giá 24h & Nhận định AI'}
                  </span>
                </div>
                <button className="close-btn" onClick={() => setActivePopup(null)}>
                  <X size={18} />
                </button>
              </div>

              <div className="ai-popup-body custom-scrollbar">
                
                {/* VIEW 1: TIN TỨC */}
                {activePopup === 'news' && (
                  <div className="content-frame">
                    {loading && newsList.length === 0 ? (
                      <div className="loading-state">Đang tải tin tức...</div>
                    ) : newsList.length === 0 ? (
                      <div className="loading-state">Chưa có tin tức mới.</div>
                    ) : (
                      newsList.map((item, idx) => (
                        <div key={idx} className="news-card">
                          <div className="news-title">
                            <a href={item.url} target="_blank" rel="noreferrer" className="hover:text-[#F0B90B]">
                              {item.title}
                            </a>
                          </div>
                          <div className="news-meta">
                            <span className="news-source">{item.source}</span>
                            <span className="news-time">{item.time}</span>
                          </div>
                        </div>
                      ))
                    )}
                  </div>
                )}

                {/* VIEW 2: DỰ BÁO (ALERTS) */}
                {activePopup === 'price' && (
                  <div className="content-frame">
                    <div className="mb-2 text-xs text-gray-500 italic text-center">
                      Dữ liệu được AI phân tích & cập nhật tự động mỗi 30 phút.
                    </div>

                    {loading && aiAlerts.length === 0 ? (
                      <div className="loading-state">
                        <div className="typing-indicator"><span></span><span></span><span></span></div>
                        <p>Đang tải nhận định...</p>
                      </div>
                    ) : aiAlerts.length === 0 ? (
                      <div className="loading-state">Hệ thống đang khởi động, vui lòng thử lại sau 30s.</div>
                    ) : (
                      aiAlerts.map((alert, idx) => {
                        const isPump = alert.change >= 0;
                        const colorClass = isPump ? "text-[#0ECB81]" : "text-[#F6465D]";
                        const arrow = isPump ? "↑" : "↓";

                        return (
                          <div key={idx} className="ai-card">
                            <div className="card-header border-b border-[#2B3139] pb-2 mb-2">
                              <div className="flex items-center gap-2">
                                {/* Icon Coin */}
                                <div className="w-6 h-6 rounded-full bg-[#2B3139] flex items-center justify-center text-[10px] font-bold text-[#EAECEF] border border-[#474D57]">
                                  {alert.symbol[0]}
                                </div>
                                <span className="coin-name text-sm">{alert.symbol}/USDT</span>
                              </div>

                              <div className={`text-xs font-mono font-bold ${colorClass} bg-[#2B3139] px-2 py-1 rounded`}>
                                {arrow} {Math.abs(alert.change)}%
                              </div>
                            </div>

                            <div className="card-content">
                              <p className="text-[13px] leading-relaxed text-[#B7BDC6]">
                                <span className="text-[#F0B90B] font-bold">AI Nhận định: </span>
                                {alert.analysis?.summary || "Đang cập nhật..."}
                              </p>
                              <div className="mt-2 text-[10px] text-[#5E6673] flex justify-between">
                                <span>Giá: ${alert.price}</span>
                                <span>Cập nhật: {alert.timestamp}</span>
                              </div>
                            </div>
                          </div>
                        );
                      })
                    )}
                  </div>
                )}

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