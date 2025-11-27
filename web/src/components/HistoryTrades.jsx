import React, { useEffect, useState } from "react";
import { api } from "../api/client";
import { useNavigate } from "react-router-dom";
import '../styles/Admin_manageUser.css'; // Tận dụng lại CSS của Admin Dashboard

export default function HistoryTrades() {
  const [allOrders, setAllOrders] = useState([]);
  const [loading, setLoading] = useState(true);
  const navigate = useNavigate();

  useEffect(() => {
    fetchAllOrders();
  }, []);

  const fetchAllOrders = async () => {
    setLoading(true);
    try {
        // 1. Lấy danh sách tất cả Users
        const users = await api.getAllUsers();
        
        // 2. Gọi song song API lấy order của từng user
        const orderPromises = users.map(u => api.getOpenOrders(u.user_id));
        const ordersResults = await Promise.all(orderPromises);

        // 3. Gộp kết quả lại và thêm thông tin Username vào order
        let combinedOrders = [];
        ordersResults.forEach((orders, index) => {
            if (Array.isArray(orders)) {
                // Gắn thêm username vào mỗi order để biết của ai
                const ordersWithUser = orders.map(o => ({
                    ...o,
                    username: users[index].username 
                }));
                combinedOrders = [...combinedOrders, ...ordersWithUser];
            }
        });

        // 4. Sắp xếp theo thời gian mới nhất
        combinedOrders.sort((a, b) => b.timestamp - a.timestamp);
        setAllOrders(combinedOrders);

    } catch (err) {
        console.error("Lỗi tải lịch sử trade:", err);
    }
    setLoading(false);
  };

  const handleCancelOrder = async (orderId, userId) => {
      if(!window.confirm("Bạn muốn hủy lệnh này?")) return;
      
      const [success, msg] = await api.cancelOrder(orderId, userId);
      if(success) {
          fetchAllOrders(); // Reload lại danh sách
      } else {
          alert("Lỗi: " + msg);
      }
  };

  // Helper format
  const formatTime = (ts) => new Date(ts * 1000).toLocaleString('vi-VN');
  const formatPrice = (num) => new Intl.NumberFormat('en-US').format(num);

  return (
    <div className="admin-container">
      <div className="admin-content-wrapper">
        
        {/* Header */}
        <div className="admin-header mb-spacing">
          <h1 className="page-title">System Open Orders</h1>
          
          <button onClick={() => navigate("/admin")} className="btn-back">
              <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{marginRight: '8px'}}>
                  <path d="m12 19-7-7 7-7"/>
                  <path d="M19 12H5"/>
              </svg>
              Back to Dashboard
          </button>
        </div>

        {/* Table */}
        <div className="table-wrapper">
          <table className="user-table">
              <thead>
              <tr>
                  <th style={{width: '15%'}}>Thời gian</th>
                  <th style={{width: '15%'}}>User</th>
                  <th style={{width: '10%'}}>Cặp</th>
                  <th style={{width: '10%'}}>Loại</th>
                  <th style={{width: '15%', textAlign: 'right'}}>Giá (USD)</th>
                  <th style={{width: '15%', textAlign: 'right'}}>Số lượng</th>
                  <th style={{width: '20%', textAlign: 'center'}}>Hành động</th>
              </tr>
              </thead>
              <tbody>
                {loading ? (
                    <tr><td colSpan="7" style={{textAlign: 'center', padding: '20px'}}>Đang tải dữ liệu...</td></tr>
                ) : allOrders.length === 0 ? (
                    <tr><td colSpan="7" style={{textAlign: 'center', padding: '20px', color: '#848E9C'}}>Không có lệnh nào đang mở.</td></tr>
                ) : (
                    allOrders.map((order) => (
                        <tr key={order.order_id}>
                            <td style={{color: '#848E9C', fontSize: '13px'}}>{formatTime(order.timestamp)}</td>
                            <td style={{fontWeight: '600', color: '#EAECEF'}}>{order.username}</td>
                            <td style={{color: '#F0B90B'}}>{order.symbol}</td>
                            <td>
                                <span style={{
                                    color: order.side === 'buy' ? '#0ECB81' : '#F6465D',
                                    fontWeight: 'bold', textTransform: 'uppercase'
                                }}>
                                    {order.side}
                                </span>
                            </td>
                            <td style={{textAlign: 'right', fontFamily: 'monospace'}}>{formatPrice(order.price)}</td>
                            <td style={{textAlign: 'right', fontFamily: 'monospace'}}>{order.amount}</td>
                            <td className="action-cell" style={{textAlign: 'center'}}>
                                <button 
                                    className="btn-delete"
                                    onClick={() => handleCancelOrder(order.order_id, order.user_id)}
                                    title="Hủy lệnh này"
                                >
                                    Hủy Lệnh
                                </button>
                            </td>
                        </tr>
                    ))
                )}
              </tbody>
          </table>
        </div>

      </div>
    </div>
  );
}