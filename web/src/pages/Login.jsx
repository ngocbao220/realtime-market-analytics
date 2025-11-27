import React, { useState } from "react";
import { api } from "../api/client";
import { useNavigate } from "react-router-dom"; // Dùng hook navigate thay vì window.location

export default function Login() {
  const [username, setUsername] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const navigate = useNavigate();

  const handleLogin = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError("");
    
    try {
      // 1. Gọi API đăng nhập
      const user = await api.loginOrRegister(username);
      
      if (user) {
        // 2. Lưu thông tin vào LocalStorage
        localStorage.setItem("user", JSON.stringify(user));
        
        // 3. Phân quyền chuyển hướng
        if (user.username === "admin") {
            navigate("/admin"); // Chuyển sang Admin Dashboard
        } else {
            navigate("/dashboard"); // Chuyển sang User Dashboard (Trading)
        }
      } else {
        setError("Đăng nhập thất bại!");
      }
    } catch (err) {
      setError("Có lỗi xảy ra, vui lòng thử lại.");
    }
    setLoading(false);
  };

  // --- STYLES DARK MODE ---
  const styles = {
    container: {
        display: "flex", justifyContent: "center", alignItems: "center", 
        height: "100vh", backgroundColor: "#161a1e", color: "#EAECEF"
    },
    formBox: {
        width: "100%", maxWidth: "360px", padding: "32px",
        backgroundColor: "#1e2329", borderRadius: "6px",
        boxShadow: "0 4px 12px rgba(0,0,0,0.2)"
    },
    input: {
        width: "100%", padding: "12px", backgroundColor: "#2B3139",
        border: "1px solid #474D57", borderRadius: "4px",
        color: "white", fontSize: "14px", outline: "none", 
        boxSizing: "border-box", marginBottom: "20px"
    },
    button: {
        width: "100%", padding: "12px", backgroundColor: "#F0B90B",
        border: "none", borderRadius: "4px", color: "#1e2329",
        fontSize: "16px", fontWeight: "bold", cursor: "pointer"
    }
  };

  return (
    <div style={styles.container}>
        <div style={styles.formBox}>
        <h2 style={{ textAlign: "center", marginBottom: "24px" }}>Đăng nhập</h2>
        <form onSubmit={handleLogin}>
            <label style={{display: 'block', marginBottom: '8px', color: '#848E9C'}}>Username</label>
            <input
                type="text"
                value={username}
                onChange={e => setUsername(e.target.value)}
                required
                style={styles.input}
                placeholder="Ví dụ: admin"
            />
            <button type="submit" disabled={loading} style={styles.button}>
                {loading ? "Đang xử lý..." : "Vào Sàn"}
            </button>
            {error && <p style={{ color: "#F6465D", marginTop: 12, textAlign: "center" }}>{error}</p>}
        </form>
        </div>
    </div>
  );
}