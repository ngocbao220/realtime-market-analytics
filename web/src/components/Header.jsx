import React, { useEffect, useState, useRef } from 'react';
import { User, X, TrendingUp, Newspaper, FileText } from 'lucide-react';
import { api } from '../api/client';
import '../styles/Header.css';

const API_BASE_URL = "http://34.124.203.62:8000";

const Header = () => {
  const [tickers, setTickers] = useState([]);
  const ws = useRef(null);

  // --- STATE QUẢN LÝ POPUP & DATA ---
  const [activePopup, setActivePopup] = useState(null); // 'news' | 'price' | null
  
  // Data cho nút Dự báo
  const [aiAlerts, setAiAlerts] = useState([]); 
  
  // Data cho nút Tin tức (Dạng tóm tắt văn bản)
  const [newsSummary, setNewsSummary] = useState({ summary: "", time: "" });
  
  const [loading, setLoading] = useState(false);

  // 1. WEBSOCKET TICKER (Giữ nguyên logic cũ)
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

  // 2. FETCH DATA KHI MỞ POPUP (Logic mới)
  useEffect(() => {
    let interval;
    const fetchData = async () => {
      // Nếu không mở popup nào thì không fetch
      if (!activePopup) return;

      setLoading(true);
      try {
        let url = '';
        if (activePopup === 'price') url = `${API_BASE_URL}/narrative/alerts`;
        else if (activePopup === 'news') url = `${API_BASE_URL}/narrative/news-summary`;

        if (url) {
          const res = await fetch(url);
          
          // Kiểm tra lỗi HTTP (404, 500...)
          if (!res.ok) {
            console.error(`Fetch failed: ${res.status}`);
            setLoading(false);
            return;
          }

          const data = await res.json();

          // Cập nhật State tương ứng
          if (activePopup === 'price') {
            setAiAlerts(Array.isArray(data) ? data : []);
          } else if (activePopup === 'news') {
            // Data trả về format { summary: "...", time: "..." }
            setNewsSummary(data || { summary: "", time: "" });
          }
        }
      } catch (e) {
        console.error("API Error:", e);
      } finally {
        setLoading(false);
      }
    };

    if (activePopup) {
      fetchData(); // Gọi ngay khi mở
      interval = setInterval(fetchData, 30000); // Tự refresh mỗi 30s
    }

    return () => clearInterval(interval);
  }, [activePopup]);

  return (
    <header className="header-container">
      {/* --- LEFT SECTION (Logo, Nav, Ticker) --- */}
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

      {/* --- RIGHT SECTION (AI Buttons & Popup) --- */}
      <div className="right-section">
        <div className="ai-actions-group">
          
          {/* NÚT 1: TIN TỨC */}
          <button
            className={`action-btn ${activePopup === 'news' ? 'active' : ''}`}
            onClick={() => setActivePopup(activePopup === 'news' ? null : 'news')}
          >
            <Newspaper size={16} />
            <span>Tin tức</span>
          </button>

          {/* NÚT 2: DỰ BÁO */}
          <button
            className={`action-btn ${activePopup === 'price' ? 'active' : ''}`}
            onClick={() => setActivePopup(activePopup === 'price' ? null : 'price')}
          >
            <TrendingUp size={16} />
            <span>Dự báo</span>
          </button>

          {/* --- KHUNG POPUP HIỂN THỊ NỘI DUNG --- */}
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
                    {activePopup === 'news' ? 'Tổng hợp Tin tức (7 ngày)' : 'Dự báo & Phân tích (24h)'}
                  </span>
                </div>
                <button className="close-btn" onClick={() => setActivePopup(null)}>
                  <X size={18} />
                </button>
              </div>

              <div className="ai-popup-body custom-scrollbar">
                
                {/* === VIEW 1: TIN TỨC (TÓM TẮT VĂN BẢN) === */}
                {activePopup === 'news' && (
                  <div className="content-frame">
                    {loading && !newsSummary.summary ? (
                      <div className="loading-state">
                         <div className="typing-indicator"><span></span><span></span><span></span></div>
                         <p>AI đang đọc tin tức tuần qua...</p>
                      </div>
                    ) : (
                      <div className="text-block">
                          {/* Header nhỏ hiển thị thời gian cập nhật */}
                          <div className="flex justify-between items-center mb-3 border-b border-[#474D57] pb-2">
                              <span className="text-[#F0B90B] font-bold text-xs uppercase tracking-wide">
                                  ⚡ AI Market Recap
                              </span>
                              <span className="text-[10px] text-[#848E9C]">
                                  Cập nhật: {newsSummary.time || "Mới nhất"}
                              </span>
                          </div>

                          {/* Nội dung tóm tắt (Hỗ trợ xuống dòng) */}
                          <div className="text-[#EAECEF] text-[13px] leading-6 whitespace-pre-line text-justify">
                              {newsSummary.summary || "Chưa có dữ liệu tổng hợp. Vui lòng quay lại sau."}
                          </div>

                          <div className="mt-4 pt-2 border-t border-[#2B3139] text-[10px] text-[#5E6673] italic">
                              *Tổng hợp tự động từ dữ liệu On-chain & Báo chí.
                          </div>
                      </div>
                    )}
                  </div>
                )}

                {/* === VIEW 2: DỰ BÁO (LIST COIN CARDS) === */}
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
                      <div className="loading-state">Hệ thống đang khởi động... (Vui lòng chờ 1-2 phút)</div>
                    ) : (
                      aiAlerts.map((alert, idx) => {
                        // Kiểm tra dữ liệu an toàn
                        if (!alert || !alert.symbol) return null;

                        // 1. Cắt đuôi USDT để hiển thị đẹp (BTCUSDT -> BTC)
                        const displaySymbol = alert.symbol.replace("USDT", "");
                        
                        // 2. Tính toán màu sắc
                        const changeVal = alert.change ? parseFloat(alert.change) : 0;
                        const isPump = changeVal >= 0;
                        const colorClass = isPump ? "text-[#0ECB81]" : "text-[#F6465D]";
                        const arrow = isPump ? "↑" : "↓";

                        // 3. Xử lý văn bản nhận định (Xóa prefix thừa nếu có)
                        let summary = alert.analysis?.summary || "Đang cập nhật...";
                        summary = summary.replace(/^Nhận định:\s*/i, "");

                        return (
                          <div key={idx} className="ai-card">
                            <div className="card-header border-b border-[#2B3139] pb-2 mb-2">
                              <div className="flex items-center gap-2">
                                {/* Icon Coin giả lập */}
                                <div className="w-6 h-6 rounded-full bg-[#2B3139] flex items-center justify-center text-[10px] font-bold text-[#EAECEF] border border-[#474D57]">
                                  {displaySymbol[0]}
                                </div>
                                <span className="coin-name text-sm">{displaySymbol}/USDT</span>
                              </div>

                              <div className={`text-xs font-mono font-bold ${colorClass} bg-[#2B3139] px-2 py-1 rounded`}>
                                {arrow} {Math.abs(changeVal).toFixed(2)}%
                              </div>
                            </div>

                            <div className="card-content">
                              <p className="text-[13px] leading-relaxed text-[#B7BDC6]">
                                <span className="text-[#F0B90B] font-bold">AI Nhận định: </span>
                                {summary}
                              </p>
                              <div className="mt-2 text-[10px] text-[#5E6673] flex justify-between">
                                {/* Format giá tiền có dấu phẩy */}
                                <span>Giá: ${alert.current_price || alert.price}</span>
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