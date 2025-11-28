import React, { useEffect, useState, useRef, useMemo } from "react";
import { api } from "../api/client";
import { useNavigate } from "react-router-dom";
import '../styles/Admin_manageUser.css';

export default function HistoryTrades() {
  const navigate = useNavigate();

  // --- STATE ---
  const [users, setUsers] = useState([]);
  const [selectedUserId, setSelectedUserId] = useState("ALL");
  
  // Dữ liệu thô từ WebSocket (Raw Data)
  const [rawOpenOrders, setRawOpenOrders] = useState([]);
  const [rawHistory, setRawHistory] = useState([]);
  const [rawTrades, setRawTrades] = useState([]);
  
  const [isLoading, setIsLoading] = useState(false);
  const [activeTab, setActiveTab] = useState("open"); 

  // Refs
  const wsRef = useRef(null);

  // 1. Load Users
  useEffect(() => {
    api.getAllUsers().then(setUsers).catch(console.error);
  }, []);

  // 2. WebSocket Logic (Tách biệt hoàn toàn khỏi Logic hiển thị)
  useEffect(() => {
    // Reset data khi đổi User
    setRawOpenOrders([]);
    setRawHistory([]);
    setRawTrades([]);
    setIsLoading(true);

    if (wsRef.current) wsRef.current.close();

    const endpoint = selectedUserId === "ALL" 
        ? "/orders/ws/admin/monitor" 
        : `/orders/ws/history/${selectedUserId}`; // Dùng endpoint history để lấy tất cả

    // Nếu chọn Single User, cần thêm 1 WS nữa cho OpenOrders hoặc dùng polling
    // Tuy nhiên để code gọn cho Admin, ta ưu tiên Logic Monitor ALL
    // (Ở đây tôi setup theo logic Monitor mà bạn đang dùng chính)
    
    console.log(`📡 Connecting WS: ${endpoint}`);
    const wsUrl = api.getWebSocketUrl(selectedUserId === "ALL" ? "/orders/ws/admin/monitor" : `/orders/ws/admin/monitor`); 
    // LƯU Ý: Với Admin Dashboard, tốt nhất luôn dùng kênh Monitor để xem dữ liệu chuẩn
    
    wsRef.current = new WebSocket(wsUrl);
    wsRef.current.onopen = () => setIsLoading(false);
    
    wsRef.current.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            
            // Cập nhật State Thô (Raw State)
            // Backend trả về: { open_orders, history, trades }
            if (data.open_orders) setRawOpenOrders(data.open_orders);
            if (data.history) setRawHistory(data.history);
            if (data.trades) setRawTrades(data.trades);
            
            // Nếu là Single User mode mà API trả format khác, bạn cần handle riêng
            // Nhưng tốt nhất Backend Admin Monitor nên hỗ trợ filter theo UserID luôn
        } catch (e) {
            console.error("WS Parse Error", e);
        }
    };

    return () => {
        if (wsRef.current) wsRef.current.close();
    };
  }, [selectedUserId]);


  // 3. LOGIC LỌC DỮ LIỆU (Client-Side Guard) - QUAN TRỌNG NHẤT
  // Dùng useMemo để tính toán lại mỗi khi rawData hoặc activeTab thay đổi
  // Giúp đổi tab cực nhanh và không bị lag/sai data
  const displayData = useMemo(() => {
      let data = [];
      
      if (activeTab === 'open') {
          // FIX CỨNG: Chỉ lấy lệnh có status NEW hoặc PARTIAL
          // Lọc bỏ mọi lệnh FILLED/CANCELLED dù backend có gửi nhầm
          data = rawOpenOrders.filter(o => 
              o.status === 'NEW' || o.status === 'PARTIAL'
          );
          
          // Nếu đang chọn Single User (không phải ALL), lọc thêm theo ID ở Client (nếu dùng chung kênh Monitor)
          if (selectedUserId !== "ALL") {
              data = data.filter(o => String(o.user_id) === String(selectedUserId));
          }

      } else if (activeTab === 'history') {
          data = rawHistory;
          if (selectedUserId !== "ALL") {
              data = data.filter(o => String(o.user_id) === String(selectedUserId));
          }
      } else if (activeTab === 'trades') {
          // Chuẩn hóa Trades
          data = rawTrades.map(t => ({
              ...t,
              amount: t.amount || t.qty || t.quantity || 0,
              price: t.price || t.Price || 0,
              time: t.time || t.timestamp || t.TradeTime,
              side: t.side || t.Side || (t.isBuyer ? 'buy' : 'sell')
          }));
          if (selectedUserId !== "ALL") {
              data = data.filter(t => String(t.user_id) === String(selectedUserId));
          }
      }
      
      return data;
  }, [activeTab, rawOpenOrders, rawHistory, rawTrades, selectedUserId]);


  // --- Actions & Helpers ---
  const handleCancelOrder = async (orderId, ownerId) => {
      const uid = ownerId || selectedUserId;
      if(!window.confirm(`Hủy lệnh ${orderId}?`)) return;
      await api.cancelOrder(orderId, uid);
      // Không cần alert hay reload, WS sẽ tự cập nhật
  };

  const formatTime = (ts) => {
      if (!ts) return "-";
      const date = typeof ts === 'number' ? new Date(ts > 10000000000 ? ts : ts * 1000) : new Date(ts);
      return date.toLocaleString('vi-VN');
  };
  const formatPrice = (n) => n ? new Intl.NumberFormat('en-US').format(n) : '0';

  const renderSide = (side) => {
      const s = String(side || "").toLowerCase();
      const isBuy = s.includes('buy') || s.includes('bid');
      return <span style={{color: isBuy ? '#0ECB81' : '#F6465D', fontWeight: 'bold'}}>{isBuy ? 'MUA' : 'BÁN'}</span>;
  };

  return (
    <div className="admin-container">
      <div className="admin-content-wrapper">
        <div className="admin-header mb-spacing" style={{flexDirection: 'column', alignItems: 'flex-start', gap: '15px'}}>
          <div style={{display: 'flex', justifyContent: 'space-between', width: '100%'}}>
            <h1 className="page-title">Realtime Market Monitor</h1>
            <button onClick={() => navigate("/admin")} className="btn-back">Dashboard</button>
          </div>

          <div style={{display: 'flex', alignItems: 'center', gap: '10px', background: '#2B3139', padding: '10px', borderRadius: '4px', width: '100%'}}>
             <span style={{color: '#848E9C'}}>Target:</span>
             <select 
                className="user-select-dropdown"
                value={selectedUserId}
                onChange={(e) => setSelectedUserId(e.target.value)}
                style={{ minWidth: '250px', background: '#1E2329', color: '#EAECEF' }}
             >
                 <option value="ALL">🔴 ALL USERS (Global Stream)</option>
                 {users.map(u => <option key={u.user_id} value={u.user_id}>{u.username}</option>)}
             </select>
             <span style={{marginLeft: 'auto', fontSize: '12px', color: '#0ECB81'}}>● Live</span>
          </div>
        </div>

        {/* Tabs */}
        <div className="tabs-container" style={{display: 'flex', gap: '2px'}}>
            {[
                {k: 'open', l: 'OPEN ORDERS', count: rawOpenOrders.length}, 
                {k: 'history', l: 'ORDER HISTORY', count: rawHistory.length}, 
                {k: 'trades', l: 'TRADE HISTORY', count: rawTrades.length}
            ].map(tab => (
                <button
                    key={tab.k}
                    onClick={() => setActiveTab(tab.k)}
                    style={{
                        padding: '12px 24px',
                        background: activeTab === tab.k ? '#1E2329' : '#2B3139',
                        color: activeTab === tab.k ? '#F0B90B' : '#848E9C',
                        borderTop: activeTab === tab.k ? '2px solid #F0B90B' : '2px solid transparent',
                        cursor: 'pointer', fontWeight: 'bold'
                    }}
                >
                    {tab.l} 
                    {/* Badge đếm số lượng */}
                    {selectedUserId === "ALL" && (
                        <span style={{marginLeft: '8px', fontSize: '11px', background: '#474D57', padding: '2px 6px', borderRadius: '10px', color: 'white'}}>
                            {tab.count}
                        </span>
                    )}
                </button>
            ))}
        </div>

        {/* Table */}
        <div className="table-wrapper" style={{background: '#1E2329', minHeight: '400px'}}>
          <table className="user-table">
              <thead>
              <tr>
                  <th>Time</th>
                  <th>User</th>
                  <th>Symbol</th>
                  <th>Side</th>
                  <th style={{textAlign: 'right'}}>Price</th>
                  <th style={{textAlign: 'right'}}>Amount</th>
                  
                  {activeTab !== 'trades' && <th>Status</th>}
                  {activeTab === 'history' && <th style={{textAlign: 'right'}}>Filled</th>}
                  {activeTab === 'open' && <th style={{textAlign: 'center'}}>Action</th>}
              </tr>
              </thead>
              <tbody>
                {isLoading && <tr><td colSpan="10" style={{textAlign: 'center', padding: '20px'}}>Connecting...</td></tr>}

                {!isLoading && displayData.length === 0 && (
                    <tr><td colSpan="10" className="empty-cell">No Data</td></tr>
                )}

                {displayData.map((item, idx) => (
                    // KEY LÀ QUAN TRỌNG NHẤT ĐỂ KHÔNG BỊ LỖI RENDER
                    // Dùng activeTab trong key để ép React vẽ lại hàng mới hoàn toàn khi đổi tab
                    <tr key={`${activeTab}-${item.order_id || item.trade_id || idx}`}>
                        <td style={{color: '#848E9C', fontSize: '13px'}}>{formatTime(item.time)}</td>
                        <td style={{fontWeight: '600', color: '#EAECEF'}}>{item.username || item.user_id}</td>
                        <td style={{color: '#F0B90B'}}>{item.symbol}</td>
                        <td>{renderSide(item.side)}</td>
                        <td className="font-mono text-right">{formatPrice(item.price)}</td>
                        <td className="font-mono text-right">{item.amount}</td>

                        {activeTab !== 'trades' && (
                            <td>
                                <span style={{
                                    color: item.status === 'FILLED' ? '#0ECB81' : 
                                           item.status === 'CANCELLED' ? '#F6465D' : '#EAECEF'
                                }}>
                                    {item.status || 'NEW'}
                                </span>
                            </td>
                        )}

                        {activeTab === 'history' && <td className="font-mono text-right">{item.filled || 0}</td>}
                        
                        {activeTab === 'open' && (
                            <td className="action-cell text-center">
                                <button className="btn-delete" onClick={() => handleCancelOrder(item.order_id, item.user_id)}>
                                    Hủy
                                </button>
                            </td>
                        )}
                    </tr>
                ))}
              </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}