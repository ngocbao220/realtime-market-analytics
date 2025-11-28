import React, { useState, useEffect, useRef } from 'react';
import '../styles/User_History.css';
import { api } from '../api/client';

const UserHistory = () => {
  const [activeTab, setActiveTab] = useState('OpenOrders');
  const [user, setUser] = useState(null);
  
  // State dữ liệu
  const [openOrders, setOpenOrders] = useState([]); 
  const [orderHistory, setOrderHistory] = useState([]);
  const [tradeHistory, setTradeHistory] = useState([]);
  const [balance, setBalance] = useState(null);

  // Refs để giữ kết nối WebSocket
  const wsOpenOrdersRef = useRef(null);
  const wsHistoryRef = useRef(null);

  // 1. Lấy User info từ LocalStorage
  useEffect(() => {
    const storedUser = localStorage.getItem("user");
    if (storedUser) setUser(JSON.parse(storedUser));
  }, []);

  const isLoggedIn = !!user;

  // --------------------------------------------------------
  // 2. WEBSOCKET 1: OPEN ORDERS (Lệnh đang chờ khớp)
  // Endpoint: /orders/ws/{userId}
  // --------------------------------------------------------
  useEffect(() => {
    if (!isLoggedIn || !user) return;
    
    const userId = user.id || user.user_id;
    const wsUrl = api.getWebSocketUrl(`/orders/ws/${userId}`);
    
    wsOpenOrdersRef.current = new WebSocket(wsUrl);
    
    wsOpenOrdersRef.current.onopen = () => {
        // console.log("✅ WS OpenOrders Connected");
    };
    
    wsOpenOrdersRef.current.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (Array.isArray(data)) {
            setOpenOrders(data);
        }
      } catch (e) {
        console.error("WS OpenOrders Parse Error", e);
      }
    };

    wsOpenOrdersRef.current.onclose = () => {
        // console.log("WS OpenOrders Disconnected");
    };

    return () => {
      if (wsOpenOrdersRef.current) wsOpenOrdersRef.current.close();
    };
  }, [isLoggedIn, user]);

  // --------------------------------------------------------
  // 3. WEBSOCKET 2: HISTORY (Lệnh cũ & Khớp lệnh)
  // Endpoint: /orders/ws/history/{userId}
  // --------------------------------------------------------
  useEffect(() => {
    if (!isLoggedIn || !user) return;
    
    const userId = user.id || user.user_id;
    const wsUrl = api.getWebSocketUrl(`/orders/ws/history/${userId}`);
    
    wsHistoryRef.current = new WebSocket(wsUrl);

    wsHistoryRef.current.onopen = () => {
        // console.log("✅ WS History Connected");
    };

    wsHistoryRef.current.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            if (data.orders && Array.isArray(data.orders)) {
                setOrderHistory(data.orders);
            }
            if (data.trades && Array.isArray(data.trades)) {
                setTradeHistory(data.trades);
            }
        } catch (e) {
            console.error("WS History Parse Error", e);
        }
    };

    wsHistoryRef.current.onclose = () => {
        // console.log("WS History Disconnected");
    };

    return () => {
        if (wsHistoryRef.current) wsHistoryRef.current.close();
    };
  }, [isLoggedIn, user]);

  // --------------------------------------------------------
  // 4. API: BALANCE
  // --------------------------------------------------------
  useEffect(() => {
    if (!isLoggedIn || !user || activeTab !== 'Balance') return;
    
    const userId = user.id || user.user_id;
    const fetchBalance = async () => {
        try {
            const data = await api.getUserInfo(userId);
            setBalance(data);
        } catch (error) {
            console.error("Failed to fetch balance:", error);
        }
    };
    fetchBalance();
    
  }, [activeTab, isLoggedIn, user, openOrders]); 


  // 5. Xử lý Hủy lệnh
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
    const s = String(side).toLowerCase();
    const isBuy = s === 'bids' || s === 'buy' || s === 'bid';
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
    { id: 'Balance', label: 'Balance' },
  ];

  // --- RENDER TABLES (Đã xóa các class text-right/text-left để ăn theo CSS Center) ---

  // 1. Open Orders
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
                    <th>Action</th>
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
                        <td>
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

  // 2. Order History
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
                    <th>Filled</th>
                </tr>
            </thead>
            <tbody>
                {orderHistory.map((order, index) => (
                    <tr key={index}>
                        <td style={{fontWeight: 'bold', color: '#eaecef'}}>{order.symbol}</td>
                        <td>{renderSide(order.side)}</td>
                        <td>{order.price?.toLocaleString()}</td>
                        <td>{order.amount}</td>
                        <td>
                            <span className={order.status === 'CANCELLED' ? 'text-red-400' : 'text-green-400'}>
                                {order.status}
                            </span>
                        </td>
                        <td>{order.filled || 0}</td>
                    </tr>
                ))}
            </tbody>
        </table>
    );
  };

  // 3. Trade History
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
                    <th>Time</th>
                </tr>
            </thead>
            <tbody>
                {tradeHistory.map((trade, index) => (
                    <tr key={index}>
                        <td style={{fontWeight: 'bold', color: '#eaecef'}}>{trade.symbol}</td>
                        <td>{renderSide(trade.side)}</td>
                        <td>{trade.price?.toLocaleString()}</td>
                        <td>{trade.amount}</td>
                        <td>
                            {trade.time && !isNaN(trade.time) && String(trade.time).length > 8 
                                ? new Date(trade.time * 1000).toLocaleTimeString() 
                                : trade.time}
                        </td>
                    </tr>
                ))}
            </tbody>
        </table>
    );
  };

  // 4. Balance
  const renderBalance = () => {
    if (!balance) return <div className="empty-state">Loading Balance...</div>;

    return (
        <table className="history-table balance-table">
            <thead>
                <tr>
                    <th>Asset</th>
                    <th>Available Balance</th>
                    <th>In Order</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>
                        <span style={{fontWeight: 'bold', color: '#f2a900', fontSize: '1.1em'}}>BTC</span>
                    </td>
                    <td className="font-number">
                        {balance.btc?.toLocaleString(undefined, { minimumFractionDigits: 6 })}
                    </td>
                    <td className="font-number" style={{ color: '#848e9c' }}>
                        {balance.reserved_btc?.toLocaleString(undefined, { minimumFractionDigits: 6 })}
                    </td>
                </tr>
                <tr>
                    <td>
                        <span style={{fontWeight: 'bold', color: '#00cc66', fontSize: '1.1em'}}>USD</span>
                    </td>
                    <td className="font-number">
                        {balance.usd?.toLocaleString(undefined, { minimumFractionDigits: 2 })}
                    </td>
                    <td className="font-number" style={{ color: '#848e9c' }}>
                        {balance.reserved_usd?.toLocaleString(undefined, { minimumFractionDigits: 2 })}
                    </td>
                </tr>
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
                {activeTab === 'Balance' && renderBalance()}
            </>
         )}
      </div>
    </div>
  );
};

export default UserHistory;