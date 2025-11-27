import React, { useState, useEffect, useRef } from 'react';
import '../styles/User_History.css';
import { api } from '../api/client'; // Import client đã update

const UserHistory = () => {
  const [activeTab, setActiveTab] = useState('OpenOrders');
  const [user, setUser] = useState(null);
  
  // State dữ liệu
  const [openOrders, setOpenOrders] = useState([]); 
  const [orderHistory, setOrderHistory] = useState([]);
  const [tradeHistory, setTradeHistory] = useState([]);

  const ws = useRef(null);

  // 1. Lấy User info từ LocalStorage
  useEffect(() => {
    const storedUser = localStorage.getItem("user");
    if (storedUser) setUser(JSON.parse(storedUser));
  }, []);

  const isLoggedIn = !!user;

  // 2. WebSocket: Chỉ dành cho Open Orders (Real-time)
  useEffect(() => {
    if (!isLoggedIn || !user) return;
    
    const userId = user.id || user.user_id;
    const wsUrl = api.getWebSocketUrl(`/orders/ws/${userId}`);
    
    ws.current = new WebSocket(wsUrl);
    ws.current.onopen = () => console.log("✅ WS Order Connected");
    
    ws.current.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (Array.isArray(data)) {
            setOpenOrders(data);
        }
      } catch (e) {
        console.error("WS Parse Error", e);
      }
    };

    return () => {
      if (ws.current) ws.current.close();
    };
  }, [isLoggedIn, user]);

  // 3. Fetch Data API: Dành cho History & Trade
  useEffect(() => {
    if (!isLoggedIn || !user) return;
    const userId = user.id || user.user_id;

    const fetchData = async () => {
      try {
        if (activeTab === 'OrderHistory') {
            // Gọi API lấy lịch sử lệnh
            const data = await api.getOrderHistory(userId);
            setOrderHistory(data);
        } 
        else if (activeTab === 'TradeHistory') {
            // Gọi API lấy lịch sử khớp lệnh
            const data = await api.getTradeHistory(userId);
            setTradeHistory(data);
        }
      } catch (error) {
        console.error("Failed to fetch history:", error);
      }
    };

    fetchData();
  }, [activeTab, isLoggedIn, user]);

  // 4. Xử lý Hủy lệnh (Open Orders)
  const handleCancelOrder = async (orderId) => {
    if (!window.confirm("Bạn muốn hủy lệnh này?")) return;
    const userId = user.id || user.user_id;
    
    const [success, message] = await api.cancelOrder(orderId, userId);
    if (success) {
        alert("✅ " + message);
    } else {
        alert("❌ Hủy thất bại: " + message);
    }
  };

  // Helper render màu sắc Mua/Bán
  const renderSide = (side) => {
    const isBuy = side === 'bids' || side === 'buy';
    return (
        <span className={isBuy ? 'text-green-500' : 'text-red-500'}>
            {isBuy ? 'Buy' : 'Sell'}
        </span>
    );
  };

  const tabs = [
    { id: 'OpenOrders', label: `Open Orders (${openOrders.length})` },
    { id: 'OrderHistory', label: 'Order History' },
    { id: 'TradeHistory', label: 'Trade History' },
  ];

  // --- RENDER TABLES ---

  // 1. Open Orders: Symbol, Type, Price, Amount, Status, Action
  const renderOpenOrders = () => {
    if (openOrders.length === 0) return <div className="empty-state">No Open Orders</div>;

    return (
        <table className="history-table">
            <thead>
                <tr>
                    <th>Symbol</th>
                    <th>Type</th>
                    <th>Price</th>
                    <th>Amount</th>
                    <th>Status</th>
                    <th className="text-right">Action</th>
                </tr>
            </thead>
            <tbody>
                {openOrders.map((order) => (
                    <tr key={order.order_id}>
                        <td style={{fontWeight: 'bold', color: '#eaecef'}}>{order.symbol}</td>
                        <td>{renderSide(order.side)}</td>
                        <td>{order.price?.toLocaleString()}</td>
                        <td>{order.amount}</td>
                        <td>{order.status || 'Pending'}</td>
                        <td className="text-right">
                            <button 
                                className="btn-cancel"
                                onClick={() => handleCancelOrder(order.order_id)}
                            >
                                Cancel
                            </button>
                        </td>
                    </tr>
                ))}
            </tbody>
        </table>
    );
  };

  // 2. Order History: Symbol, Type, Price, Amount, Status, Filled Amount
  const renderOrderHistory = () => {
    if (orderHistory.length === 0) return <div className="empty-state">No Order History</div>;

    return (
        <table className="history-table">
            <thead>
                <tr>
                    <th>Symbol</th>
                    <th>Type</th>
                    <th>Price</th>
                    <th>Amount</th>
                    <th>Status</th>
                    <th className="text-right">Filled Amount</th>
                </tr>
            </thead>
            <tbody>
                {orderHistory.map((order, index) => (
                    <tr key={index}>
                        <td style={{fontWeight: 'bold', color: '#eaecef'}}>{order.symbol}</td>
                        <td>{renderSide(order.side)}</td>
                        <td>{order.price?.toLocaleString()}</td>
                        <td>{order.amount}</td>
                        <td>{order.status}</td>
                        <td className="text-right">{order.filled || 0}</td>
                    </tr>
                ))}
            </tbody>
        </table>
    );
  };

  // 3. Trade History: Symbol, Type, Price, Amount, Fee, Time
  const renderTradeHistory = () => {
    if (tradeHistory.length === 0) return <div className="empty-state">No Trade History</div>;

    return (
        <table className="history-table">
            <thead>
                <tr>
                    <th>Symbol</th>
                    <th>Type</th>
                    <th>Price</th>
                    <th>Amount</th>
                    <th>Fee</th> {/* Cột mới thêm */}
                    <th className="text-right">Time</th>
                </tr>
            </thead>
            <tbody>
                {tradeHistory.map((trade, index) => (
                    <tr key={index}>
                        <td style={{fontWeight: 'bold', color: '#eaecef'}}>{trade.symbol}</td>
                        <td>{renderSide(trade.side)}</td>
                        <td>{trade.price?.toLocaleString()}</td>
                        <td>{trade.amount}</td>
                        {/* Hiển thị Fee, nếu không có thì hiện 0 */}
                        <td>{trade.fee ? trade.fee.toLocaleString() : 0}</td> 
                        <td className="text-right">
                            {new Date(trade.time * 1000).toLocaleString()}
                        </td>
                    </tr>
                ))}
            </tbody>
        </table>
    );
  };

  return (
    <div className="user-history-container">
      <div className="history-tabs">
        {tabs.map((tab) => (
          <div 
            key={tab.id} 
            className={`h-tab ${activeTab === tab.id ? 'active' : ''}`}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </div>
        ))}
      </div>

      <div className="history-content">
         {!isLoggedIn ? (
            <div className="empty-state">
               Log In or Register Now to trade
            </div>
         ) : (
            <>
                {activeTab === 'OpenOrders' && renderOpenOrders()}
                {activeTab === 'OrderHistory' && renderOrderHistory()}
                {activeTab === 'TradeHistory' && renderTradeHistory()}
            </>
         )}
      </div>
    </div>
  );
};

export default UserHistory;