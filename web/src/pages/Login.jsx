import React, { useState } from "react";
import { api } from "../api/client";

export default function Login() {
  const [username, setUsername] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const handleLogin = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError("");
    try {
      // Gọi API đăng nhập/tạo user
      const user = await api.loginOrRegister(username);
      if (user) {
        localStorage.setItem("user", JSON.stringify(user));
        window.location.href = "/dashboard"; // Chuyển hướng sang dashboard
      } else {
        setError("Đăng nhập thất bại!");
      }
    } catch (err) {
      setError("Đăng nhập thất bại!");
    }
    setLoading(false);
  };

  return (
    <form onSubmit={handleLogin} style={{ maxWidth: 320, margin: "40px auto", padding: 24, border: "1px solid #eee", borderRadius: 8 }}>
      <h2>Đăng nhập</h2>
      <input
        type="text"
        placeholder="Username"
        value={username}
        onChange={e => setUsername(e.target.value)}
        required
        style={{ width: "100%", padding: 8, marginBottom: 12 }}
      />
      <button type="submit" disabled={loading} style={{ width: "100%", padding: 8 }}>
        {loading ? "Đang xử lý..." : "Đăng nhập"}
      </button>
      {error && <p style={{ color: "red", marginTop: 12 }}>{error}</p>}
    </form>
  );
}