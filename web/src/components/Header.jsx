// Trang này hiển thị Tickers + Nút gọi AI
import React, { useState, useEffect } from "react";
export default function Header() {
    const userData = localStorage.getItem("user");
    const user = userData ? JSON.parse(userData) : null
    const [theme, setTheme] = useState(localStorage.getItem("theme") || "dark");
    const [showSetting, setShowSetting] = useState(false);

    useEffect(() => {
    document.body.style.background = theme === "dark" ? "#222" : "#fff";
    document.body.style.color = theme === "dark" ? "#fff" : "#222";
    localStorage.setItem("theme", theme);
  }, [theme])

    const handleLogout = () => {
    localStorage.removeItem("user");
    window.location.href = "/login";
  };
    const toggleTheme = () => {
    setTheme(theme === "dark" ? "light" : "dark");
  };

    return (
       <header style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "16px 32px", background: theme === "dark" ? "#222" : "#eee", color: theme === "dark" ? "#fff" : "#222" }}>
      <h2>Crypto Dashboard</h2>
      <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
        <button onClick={() => setShowSetting(!showSetting)} style={{ padding: "6px 16px", borderRadius: 4, border: "none", background: "#3498db", color: "#fff", cursor: "pointer" }}>
          Setting
        </button>
        {showSetting && (
          <div style={{ position: "absolute", top: 56, right: 32, background: theme === "dark" ? "#333" : "#fff", color: theme === "dark" ? "#fff" : "#222", border: "1px solid #ccc", borderRadius: 8, padding: 16, zIndex: 10 }}>
            <h4>Cài đặt giao diện</h4>
            <button onClick={toggleTheme} style={{ padding: "6px 16px", borderRadius: 4, border: "none", background: "#2ecc71", color: "#fff", cursor: "pointer" }}>
              Chuyển sang giao diện {theme === "dark" ? "sáng" : "tối"}
            </button>
          </div>
        )}
        {user ? (
          <>
            <span>Xin chào, <b>{user.username}</b></span>
            <button onClick={handleLogout} style={{ padding: "6px 16px", borderRadius: 4, border: "none", background: "#e74c3c", color: "#fff", cursor: "pointer" }}>
              Đăng xuất
            </button>
          </>
        ) : (
          <a href="/login" style={{ color: theme === "dark" ? "#fff" : "#222" }}>Đăng nhập</a>
        )}
      </div>
    </header>
  );
}