import React, { useEffect, useState } from "react";
import { api } from "../api/client";
import { useNavigate } from "react-router-dom";

export default function Admin_Dashboard() {
  const [users, setUsers] = useState([]);
  const navigate = useNavigate();

  useEffect(() => {
    loadUsers();
  }, []);

  const loadUsers = async () => {
    const list = await api.getAllUsers();
    setUsers(list);
  };

  const handleDelete = async (userId) => {
    if (window.confirm("Bạn chắc chắn muốn xóa user này?")) {
      const [success, msg] = await api.deleteUser(userId);
      if (success) {
        alert("Đã xóa thành công!");
        loadUsers(); // Refresh lại list
      } else {
        alert("Lỗi: " + msg);
      }
    }
  };

  const handleLogout = () => {
    localStorage.removeItem("user");
    navigate("/login");
  };

  return (
    <div style={{ padding: 20, color: "#fff", background: "#161a1e", minHeight: "100vh" }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 20 }}>
        <h1>Admin Dashboard - Quản lý người dùng</h1>
        <button onClick={handleLogout} style={{ padding: "5px 15px", cursor: "pointer", background: "#2B3139", color: "white", border: "none" }}>Đăng xuất</button>
      </div>

      <table style={{ width: "100%", borderCollapse: "collapse", border: "1px solid #333" }}>
        <thead>
          <tr style={{ background: "#2B3139" }}>
            <th style={{ padding: 10, textAlign: "left" }}>ID</th>
            <th style={{ padding: 10, textAlign: "left" }}>Username</th>
            <th style={{ padding: 10, textAlign: "center" }}>Hành động</th>
          </tr>
        </thead>
        <tbody>
          {users.map((u) => (
            <tr key={u.user_id} style={{ borderBottom: "1px solid #333" }}>
              <td style={{ padding: 10 }}>{u.user_id}</td>
              <td style={{ padding: 10 }}>{u.username}</td>
              <td style={{ padding: 10, textAlign: "center" }}>
                <button 
                  onClick={() => handleDelete(u.user_id)}
                  style={{ background: "#F6465D", color: "white", border: "none", padding: "5px 10px", cursor: "pointer", borderRadius: 4 }}
                >
                  Xóa
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}