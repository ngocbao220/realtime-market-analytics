import React, { useEffect, useState } from "react";
import { api } from "../api/client";
import { useNavigate } from "react-router-dom";
import { Settings } from "lucide-react"; 
import '../styles/Admin_manageUser.css';

export default function Manage_user() {
  const [users, setUsers] = useState([]);
  const navigate = useNavigate();

  // Danh sách các username được coi là BOT (để hiện nút Config)
  // LƯU Ý: Phải khớp chính xác tên trong Database
  const BOT_ACCOUNTS = ['marker_bot', 'taker_bot']; 
  
  // Danh sách các username KHÔNG được phép xóa
  const PROTECTED_ACCOUNTS = ['admin', 'system', ...BOT_ACCOUNTS];

  useEffect(() => {
    loadUsers();
  }, []);

  const loadUsers = async () => {
    const list = await api.getAllUsers();
    list.sort((a, b) => Number(a.user_id) - Number(b.user_id));
    setUsers(list);
  };

  const handleDelete = async (userId, username) => {
    if (window.confirm(`Bạn chắc chắn muốn xóa user: ${username}?`)) {
      const [success, msg] = await api.deleteUser(userId);
      if (success) {
        loadUsers(); 
      } else {
        alert("Lỗi: " + msg);
      }
    }
  };

  const handleConfigBot = (botName) => {
      alert(`Mở cài đặt cho: ${botName}`);
  };

  const formatNumber = (num) => {
      if (num === undefined || num === null) return "0.00";
      if (num > 1000000000000) return "∞"; 
      return new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 6 }).format(num);
  };

  return (
    <div className="admin-container">
      <div className="admin-content-wrapper">
        
        <div className="admin-header mb-spacing">
          <h1 className="page-title">Manage Users</h1>
          
          <button onClick={() => navigate("/admin")} className="btn-back">
              <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{marginRight: '8px'}}>
                  <path d="m12 19-7-7 7-7"/>
                  <path d="M19 12H5"/>
              </svg>
              Back to Dashboard
          </button>
        </div>

        <div className="table-wrapper">
          <table className="user-table">
              <thead>
              <tr>
                  <th style={{width: '5%', textAlign: 'center'}}>ID</th>
                  <th style={{width: '25%', textAlign: 'left'}}>Username</th>
                  <th style={{width: '25%', textAlign: 'right'}}>BALANCE (USD)</th>
                  <th style={{width: '25%', textAlign: 'right'}}>BALANCE (BTC)</th>
                  <th style={{width: '20%', textAlign: 'center'}}>HÀNH ĐỘNG</th>
              </tr>
              </thead>
              <tbody>
              {users.map((u) => (
                  <tr key={u.user_id}>
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

                    <td style={{textAlign: 'right', fontFamily: 'monospace', fontSize: '13px'}}>
                        <span style={{color: '#0ECB81'}}>{formatNumber(u.usd)}</span> 
                        <span style={{color: '#848E9C', fontSize: '11px', marginLeft: '4px'}}>USD</span>
                    </td>

                    <td style={{textAlign: 'right', fontFamily: 'monospace', fontSize: '13px'}}>
                        <span style={{color: '#F0B90B'}}>{formatNumber(u.btc)}</span>
                        <span style={{color: '#848E9C', fontSize: '11px', marginLeft: '4px'}}>BTC</span>
                    </td>

                    <td className="action-cell" style={{textAlign: 'center'}}>
                        
                        {/* 1. Nút Config (Chỉ hiện cho Bot) */}
                        {BOT_ACCOUNTS.includes(u.username) && (
                            <button 
                                className="btn-settings"
                                onClick={() => handleConfigBot(u.username)}
                                title="Cấu hình Bot"
                            >
                                <Settings size={14} />
                                <span>Config</span>
                            </button>
                        )}

                        {/* 2. Nút Xóa (Ẩn với Admin, System và Bot) */}
                        {!PROTECTED_ACCOUNTS.includes(u.username) && (
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