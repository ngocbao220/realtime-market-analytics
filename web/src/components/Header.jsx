import React, { useEffect, useState, useRef } from 'react';
import { Bot, User, Users, Activity, LogOut, X, Sparkles } from 'lucide-react';
import { api } from '../api/client';
import { useNavigate } from 'react-router-dom';
import '../styles/Header.css';

const API_BASE_URL = "http://34.124.203.62:8000";

const Header = () => {
  const [tickers, setTickers] = useState([]);
  const [showDropdown, setShowDropdown] = useState(false);
  const [isOpen, setIsOpen] = useState(false);
  const [aiAlerts, setAiAlerts] = useState([]);
  const [loading, setLoading] = useState(false);
  
  const ws = useRef(null);
  const dropdownRef = useRef(null);
  const navigate = useNavigate();

  const user = JSON.parse(localStorage.getItem("user") || "{}");
  const isAdmin = user && user.username === "admin";

  // Đóng menu khi click ra ngoài
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  // WebSocket Ticker
  useEffect(() => {
    const socketUrl = api.getWebSocketUrl('/ws/tickers');
    ws.current = new WebSocket(socketUrl);
    ws.current.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (Array.isArray(data)) {
          setTickers(data);
        }
      } catch (err) {
        console.error("Lỗi parse data ticker:", err);
      }
    };
    return () => { if (ws.current) ws.current.close(); };
  }, []);

  // Fetch AI Alerts
  useEffect(() => {
    let interval;
    const fetchData = async () => {
      if (!isOpen) return;

      setLoading(true);
      try {
        const res = await fetch(`${API_BASE_URL}/narrative/alerts`);
        if (res.ok) {
          const data = await res.json();
          setAiAlerts(Array.isArray(data) ? data : []);
        }
      } catch (e) {
        console.error("AI Fetch Error:", e);
      } finally {
        setLoading(false);
      }
    };

    if (isOpen) {
      fetchData();
      interval = setInterval(fetchData, 30000);
    }

    return () => clearInterval(interval);
  }, [isOpen]);

  const handleLogout = () => {
    localStorage.removeItem("user");
    navigate("/login");
  };

  return (
    <header className="header-container">
      {/* Left Section */}
      <div className="left-section">
        <div 
          className="brand-logo" 
          onClick={() => navigate(isAdmin ? "/admin" : "/dashboard")} 
          style={{cursor: 'pointer'}}
        >
          BINANCE
        </div>
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

      {/* Right Section */}
      <div className="right-section">
        {isAdmin && (
          <>
            <button className="admin-nav-btn" onClick={() => navigate("/manage-users")}>
              <Users size={16} /><span>Users</span>
            </button>
            <button className="admin-nav-btn" onClick={() => navigate("/history-trades")}>
              <Activity size={16} /><span>Trades</span>
            </button>
          </>
        )}

        {/* AI Market Insight */}
        <div className="ai-wrapper">
          <button 
            className={`ai-btn-pro ${isOpen ? 'active' : ''}`} 
            onClick={() => setIsOpen(!isOpen)}
          >
            <Sparkles size={16} className={isOpen ? "animate-spin-slow" : ""} />
            <span>AI Market Insight</span>
          </button>

          {isOpen && (
            <div className="ai-popup-pro">
              <div className="ai-popup-header">
                <div className="flex items-center gap-2">
                  <div className="live-dot"></div>
                  <span className="text-[#F0B90B] font-bold text-sm tracking-wide">
                    LIVE MARKET ANALYSIS
                  </span>
                </div>
                <button className="close-btn" onClick={() => setIsOpen(false)}>
                  <X size={18} />
                </button>
              </div>

              <div className="ai-popup-body custom-scrollbar">
                {loading && aiAlerts.length === 0 ? (
                  <div className="loading-container">
                    <div className="spinner"></div>
                    <p>AI đang quét thị trường...</p>
                  </div>
                ) : aiAlerts.length === 0 ? (
                  <div className="empty-state">
                    Hệ thống đang khởi động AI... <br/> Vui lòng chờ trong giây lát.
                  </div>
                ) : (
                  <div className="cards-list">
                    {aiAlerts.map((alert, idx) => {
                      if (!alert || !alert.symbol) return null;

                      const displaySymbol = alert.symbol.replace("USDT", "");
                      const changeVal = alert.change ? parseFloat(alert.change) : 0;
                      const isPump = changeVal >= 0;
                      const colorClass = isPump ? "text-up" : "text-down";
                      const bgClass = isPump ? "bg-up-dim" : "bg-down-dim";
                      const arrow = isPump ? "▲" : "▼";
                      
                      let summary = alert.analysis?.summary || "Đang cập nhật...";
                      summary = summary.replace(/^Nhận định:\s*/i, "");

                      return (
                        <div key={idx} className="ai-card-pro slide-in" style={{animationDelay: `${idx * 0.1}s`}}>
                          <div className="card-top">
                            <div className="coin-identity">
                              <div className="coin-avatar">{displaySymbol[0]}</div>
                              <div className="coin-info-col">
                                <span className="coin-name">{displaySymbol}/USDT</span>
                                <span className="coin-price">
                                  ${parseFloat(alert.current_price || alert.price).toLocaleString()}
                                </span>
                              </div>
                            </div>
                            <div className={`change-badge ${bgClass} ${colorClass}`}>
                              {arrow} {Math.abs(changeVal).toFixed(2)}%
                            </div>
                          </div>

                          <div className="card-body">
                            <div className="ai-label">
                              <Sparkles size={12} className="text-[#F0B90B]" />
                              <span>AI Insight:</span>
                            </div>
                            <p className="ai-text">{summary}</p>
                          </div>
                          
                          <div className="card-footer">
                            <span>Cập nhật: {alert.timestamp}</span>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            </div>
          )}
        </div>

        {/* User Menu */}
        <div className="user-menu-wrapper" ref={dropdownRef}>
          <div 
            className="user-avatar" 
            title={user.username}
            onClick={() => setShowDropdown(!showDropdown)}
          >
            <User size={16} />
          </div>

          {showDropdown && (
            <div className="logout-dropdown">
              <div className="user-info-mini">
                Hello, {user.username}
              </div>
              <button className="logout-btn" onClick={handleLogout}>
                <LogOut size={14} />
                <span>Đăng xuất</span>
              </button>
            </div>
          )}
        </div>
      </div>
    </header>
  );
};

export default Header;