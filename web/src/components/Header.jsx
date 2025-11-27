import React, { useEffect, useState, useRef } from 'react';
import { Bot, User, Users, Activity, LogOut } from 'lucide-react';
import { api } from '../api/client';
import { useNavigate } from 'react-router-dom';
import '../styles/Header.css'; 

const Header = () => {
  const [tickers, setTickers] = useState([]);
  const [showDropdown, setShowDropdown] = useState(false); // State bật/tắt menu
  const ws = useRef(null);
  const dropdownRef = useRef(null); // Ref để phát hiện click ra ngoài
  const navigate = useNavigate();

  const user = JSON.parse(localStorage.getItem("user") || "{}");
  const isAdmin = user && user.username === "admin";

  // --- Logic đóng menu khi click ra ngoài ---
  useEffect(() => {
    const handleClickOutside = (event) => {
      // Nếu click không nằm trong dropdownRef (vùng avatar + menu) -> Đóng menu
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  // --- WebSocket Ticker (Giữ nguyên) ---
  useEffect(() => {
    const socketUrl = api.getWebSocketUrl('/ws/tickers');
    ws.current = new WebSocket(socketUrl);
    ws.current.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (Array.isArray(data)) setTickers(data.slice(0, 4));
      } catch (err) {}
    };
    return () => ws.current && ws.current.close();
  }, []);

  // --- Hàm Đăng xuất ---
  const handleLogout = () => {
    localStorage.removeItem("user");
    navigate("/login");
  };

  return (
    <header className="header-container">
      {/* Left Section (Logo + Ticker) */}
      <div className="left-section">
        <div className="brand-logo" onClick={() => navigate(isAdmin ? "/admin" : "/dashboard")} style={{cursor: 'pointer'}}>
            BINANCE
        </div>
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

      {/* Right Section (Buttons + User) */}
      <div className="right-section">
        {isAdmin && (
            <>
                <button className="admin-nav-btn" onClick={() => navigate("/manage-users")}>
                    <Users size={16} /><span>Users</span>
                </button>
                <button className="admin-nav-btn" onClick={() => alert("Coming Soon!")}>
                    <Activity size={16} /><span>Trades</span>
                </button>
            </>
        )}

        <button className="ai-btn">
          <Bot size={18} /><span>AI Helper</span>
        </button>
        
        {/* --- USER MENU WRAPPER --- */}
        <div className="user-menu-wrapper" ref={dropdownRef}>
            {/* Avatar - Click để bật/tắt menu */}
            <div 
                className="user-avatar" 
                title={user.username}
                onClick={() => setShowDropdown(!showDropdown)}
            >
                <User size={16} />
            </div>

            {/* Khung chữ nhật nhỏ (Dropdown) */}
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