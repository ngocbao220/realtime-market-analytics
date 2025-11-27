import React, { useEffect, useState, useRef } from 'react';
import { User, X, TrendingUp, Newspaper, FileText } from 'lucide-react'; 
import { api } from '../api/client'; 
import '../styles/Header.css'; 

const API_BASE_URL = import.meta.env.VITE_BASE_URL;

const Header = () => {
  const [tickers, setTickers] = useState([]);
  const ws = useRef(null);
  
  // State quản lý Popup nào đang mở ('news' | 'price' | null)
  const [activePopup, setActivePopup] = useState(null);
  const [aiAlerts, setAiAlerts] = useState([]);

  // 1. WebSocket Ticker (Giữ nguyên)
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

  // 2. Fetch Data (Chỉ chạy khi mở popup 'price')
  useEffect(() => {
    let interval;
    if (activePopup === 'price') {
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
  }, [activePopup]);

  // Hàm toggle (Nếu bấm nút đang mở thì đóng, bấm nút khác thì chuyển)
  const togglePopup = (type) => {
      if (activePopup === type) {
          setActivePopup(null);
      } else {
          setActivePopup(type);
      }
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
        
        {/* Nhóm 2 Nút Chức Năng Mới */}
        <div className="ai-actions-group">
            
            {/* Nút 1: Tin Tức */}
            <button 
                className={`action-btn ${activePopup === 'news' ? 'active' : ''}`} 
                onClick={() => togglePopup('news')}
            >
                <Newspaper size={16} />
                <span>Tin tức</span>
            </button>

            {/* Nút 2: Dự Báo/Phân Tích */}
            <button 
                className={`action-btn ${activePopup === 'price' ? 'active' : ''}`} 
                onClick={() => togglePopup('price')}
            >
                <TrendingUp size={16} />
                <span>Dự báo</span>
            </button>

            {/* KHUNG HIỂN THỊ NỘI DUNG (Popup dùng chung) */}
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
                                {activePopup === 'news' ? 'Tổng hợp Tin tức (7 ngày)' : 'Phân tích Biến động (24h)'}
                            </span>
                        </div>
                        <button className="close-btn" onClick={() => setActivePopup(null)}>
                            <X size={18} />
                        </button>
                    </div>

                    <div className="ai-popup-body custom-scrollbar">
                        
                        {/* NỘI DUNG: TIN TỨC */}
                        {activePopup === 'news' && (
                            <div className="content-frame">
                                <div className="text-block">
                                    <h4 className="text-[#F0B90B] mb-2 font-bold">Điểm tin Crypto tuần qua:</h4>
                                    <p className="text-[#EAECEF] text-sm leading-6">
                                        {/* Đây là chỗ bạn "đẩy văn bản lên" sau này */}
                                        Hiện tại chưa có dữ liệu tổng hợp. Hệ thống sẽ sớm cập nhật các tin tức vĩ mô quan trọng, sự kiện Halving và các thay đổi pháp lý ảnh hưởng đến thị trường.
                                    </p>
                                    <div className="mt-4 p-3 bg-[#2B3139] rounded text-xs text-[#848E9C]">
                                        Nguồn: Tổng hợp từ các trang tin uy tín (Placeholder).
                                    </div>
                                </div>
                            </div>
                        )}

                        {/* NỘI DUNG: DỰ BÁO GIÁ */}
                        {activePopup === 'price' && (
                            <div className="content-frame">
                                {aiAlerts.length === 0 ? (
                                    <div className="loading-state">
                                        <div className="typing-indicator"><span></span><span></span><span></span></div>
                                        <p>Đang quét dữ liệu thị trường...</p>
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