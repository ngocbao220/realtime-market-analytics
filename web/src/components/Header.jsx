import React, { useEffect, useState, useRef } from 'react';
import { Bot, User, X } from 'lucide-react';
import { api } from '../api/client'; 
import '../styles/Header.css'; 

const API_BASE_URL = import.meta.env.VITE_API_URL; 

const Header = () => {
  const [tickers, setTickers] = useState([]);
  const ws = useRef(null);
  const [showAIPanel, setShowAIPanel] = useState(false);
  const [aiAlerts, setAiAlerts] = useState([]); // Khởi tạo là mảng rỗng

  useEffect(() => {
    const socketUrl = api.getWebSocketUrl('/ws/tickers');
    ws.current = new WebSocket(socketUrl);

    ws.current.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (Array.isArray(data)) {
           setTickers(data.slice(0, 4));
        }
      } catch (err) { }
    };
    return () => { if (ws.current) ws.current.close(); };
  }, []);

  // [FIX] Fetch AI Alerts an toàn hơn
  useEffect(() => {
    let interval;
    if (showAIPanel) {
        const fetchAIAlerts = async () => {
            try {
                const res = await fetch(`${API_BASE_URL}/narrative/alerts`);
                if (!res.ok) throw new Error("API Error"); // Bắt lỗi HTTP (404, 500)
                
                const data = await res.json();
                
                // [QUAN TRỌNG] Chỉ set state nếu data thực sự là Mảng
                if (Array.isArray(data)) {
                    setAiAlerts(data);
                } else {
                    console.warn("AI Alerts format invalid:", data);
                    setAiAlerts([]); // Reset về mảng rỗng nếu dữ liệu sai
                }
            } catch (e) {
                console.error("Lỗi lấy tin AI:", e);
                setAiAlerts([]); // Đảm bảo luôn là mảng để không lỗi .map()
            }
        };

        fetchAIAlerts();
        interval = setInterval(fetchAIAlerts, 5000); 
    }
    return () => clearInterval(interval);
  }, [showAIPanel]);

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
        </div>
      </div>

      <div className="right-section">
        <div style={{ position: 'relative' }}>
            <button className="ai-btn" onClick={() => setShowAIPanel(!showAIPanel)}>
                <Bot size={18} />
                <span>AI Helper</span>
            </button>

            {showAIPanel && (
                <div className="absolute top-12 right-0 w-[450px] bg-[#1E2329] border border-[#2B3139] rounded-lg shadow-2xl z-50 overflow-hidden" style={{ fontFamily: 'sans-serif' }}>
                    <div className="flex justify-between items-center p-4 border-b border-[#2B3139] bg-[#2B3139]/50">
                        <div className="flex items-center gap-2">
                            <Bot size={18} className="text-[#F0B90B]" />
                            <h3 className="font-bold text-[#EAECEF] m-0 text-base">AI Market Insights</h3>
                        </div>
                        <button onClick={() => setShowAIPanel(false)} className="text-[#848E9C] hover:text-white cursor-pointer bg-transparent border-none p-1">
                            <X size={20} />
                        </button>
                    </div>

                    <div className="max-h-[500px] overflow-y-auto custom-scrollbar p-2 space-y-2">
                        {/* Kiểm tra length an toàn */}
                        {aiAlerts.length === 0 ? (
                            <div className="p-8 text-center text-[#848E9C] text-sm">
                                <p>Đang chờ dữ liệu phân tích...</p>
                            </div>
                        ) : (
                            aiAlerts.map((alert, idx) => {
                                const isPump = alert.change >= 0;
                                const colorClass = isPump ? "text-[#0ECB81]" : "text-[#F6465D]";
                                const bgClass = isPump ? "bg-[#0ECB81]/10" : "bg-[#F6465D]/10";
                                
                                return (
                                    <div key={idx} className="bg-[#2B3139]/30 hover:bg-[#2B3139] p-3 rounded border border-transparent hover:border-[#474D57] transition-all mb-2">
                                        <div className="flex justify-between items-start mb-2">
                                            <div className="flex items-center gap-2">
                                                <span className="font-bold text-[#EAECEF] text-sm">{alert.symbol}</span>
                                                <span className={`text-xs px-1.5 py-0.5 rounded ${bgClass} ${colorClass} font-medium`}>
                                                    {isPump ? "+" : ""}{alert.change}%
                                                </span>
                                            </div>
                                            <span className="text-[10px] text-[#848E9C]">{alert.timestamp}</span>
                                        </div>
                                        <p className="text-xs text-[#B7BDC6] leading-relaxed m-0 text-left">
                                            <span className={`${colorClass} font-bold mr-1`}>AI:</span>
                                            {/* Xử lý an toàn cho chuỗi summary */}
                                            {alert.analysis?.summary ? alert.analysis.summary.replace(/\$/g, "") : "Đang cập nhật..."}
                                        </p>
                                    </div>
                                );
                            })
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