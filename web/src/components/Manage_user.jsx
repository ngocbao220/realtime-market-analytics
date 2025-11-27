import React, { useEffect, useState } from "react";
import { api } from "../api/client";
import { useNavigate } from "react-router-dom";
import '../styles/Admin_manageUser.css';

export default function Manage_user() {
  const [users, setUsers] = useState([]);
  const navigate = useNavigate();

  useEffect(() => {
    loadUsers();
  }, []);

  const loadUsers = async () => {
    const list = await api.getAllUsers();
    // Sắp xếp ID tăng dần cho dễ nhìn, đảm bảo ID là số
    list.sort((a, b) => Number(a.user_id) - Number(b.user_id));
    setUsers(list);
  };

  const handleDelete = async (userId, username) => {
    if (username === "admin") {
        alert("Không thể xóa Admin!");
        return;
    }
    if (window.confirm(`Bạn chắc chắn muốn xóa user: ${username}?`)) {
      const [success, msg] = await api.deleteUser(userId);
      if (success) {
        loadUsers(); 
      } else {
        alert("Lỗi: " + msg);
      }
    }
  };

  return (
    <div className="admin-container">
      {/* Wrapper giúp giới hạn chiều rộng và căn giữa nội dung */}
      <div className="admin-content-wrapper">
        
        {/* Header: Tiêu đề bên trái, Nút quay lại bên phải */}
        <div className="admin-header mb-spacing">
          <h1 className="page-title">Manage Users</h1>
          
          <button onClick={() => navigate("/admin")} className="btn-back">
              {/* Icon mũi tên quay lại SVG */}
              <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{marginRight: '8px'}}>
                  <path d="m12 19-7-7 7-7"/>
                  <path d="M19 12H5"/>
              </svg>
              Back to Dashboard
          </button>
        </div>

        {/* Table Container */}
        <div className="table-wrapper">
          <table className="user-table">
              <thead>
              <tr>
                  {/* Căn giữa ID */}
                  <th style={{width: '10%', textAlign: 'center'}}>ID</th>
                  <th style={{width: '60%', textAlign: 'left'}}>Username</th>
                  {/* Căn giữa Hành động */}
                  <th style={{width: '30%', textAlign: 'center'}}>Hành động</th>
              </tr>
              </thead>
              <tbody>
              {users.map((u) => (
                  <tr key={u.user_id}>
                  {/* Căn giữa nội dung ID */}
                  <td style={{textAlign: 'center', fontWeight: '500'}}>#{u.user_id}</td>
                  <td>
                      <span style={{
                          fontWeight: u.username === 'admin' ? '700' : 'normal', 
                          color: u.username === 'admin' ? '#F0B90B' : 'inherit',
                          fontSize: u.username === 'admin' ? '1.05em' : '1em'
                      }}>
                          {u.username}
                      </span>
                  </td>
                  <td className="action-cell" style={{textAlign: 'center'}}>
                      {u.username !== 'admin' && (
                          <button 
                              className="btn-delete"
                              onClick={() => handleDelete(u.user_id, u.username)}
                              title="Xóa người dùng này"
                          >
                              Xóa
                          </button>
                      )}
                  </td>
                  </tr>
              ))}
              </tbody>
          </table>
        </div>

      </div>
    </div>
  );
}