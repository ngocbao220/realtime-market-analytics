import React, { useEffect, useState, useRef } from 'react';
import '../styles/Trades.css';

const Trades = ({ symbol = "BTCUSDT" }) => {
  const [trades, setTrades] = useState([]);
  const wsRef = useRef(null);

  useEffect(() => {
    // 1. Định nghĩa URL WebSocket
    // Lưu ý: Dùng mode=history hoặc real_time tùy logic backend, 
    // nhưng limit=50 để lấp đầy danh sách lúc đầu.
    const WS_URL = `ws://localhost:8000/market/ws/trades/${symbol}?type=real&mode=real_time&limit=50`;
    
    // 2. Khởi tạo kết nối
    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
        console.log(`Connected to Trades WS: ${symbol}`);
    };

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            
            // Backend trả về 1 mảng các trade: [{}, {}, ...]
            if (Array.isArray(data)) {
                // Nếu Backend trả về trade mới nhất nằm cuối, ta cần đảo ngược (reverse) 
                // để trade mới nhất hiện lên trên cùng giao diện.
                // Dựa vào ảnh bạn gửi, có vẻ danh sách đã được sort sẵn hoặc trả về cả cụm.
                // Ta cứ set trực tiếp, nếu thấy ngược chiều thì thêm .reverse() vào.
                
                setTrades(data); 
            }
        } catch (err) {
            console.error("Error parsing Trades WS message:", err);
        }
    };

    ws.onerror = (error) => {
        console.error("WebSocket Trades Error:", error);
    };

    // 3. Cleanup khi component unmount
    return () => {
        if (wsRef.current) {
            wsRef.current.close();
            console.log(`Closed Trades WS: ${symbol}`);
        }
    };
  }, [symbol]);

  // --- FORMATTERS ---
  const formatPrice = (price) => {
    return new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(price);
  };

  const formatAmount = (num) => {
    return parseFloat(num).toFixed(5);
  };

  // Backend của bạn trả về string "16:08:42" nên hiển thị luôn, không cần format lại
  const formatTime = (timeData) => {
    return timeData; 
  };

  return (
    <div className="trades-container">
      {/* Header */}
      <div className="trades-header">
        Market Trades
      </div>

      {/* Table Head */}
      <div className="trades-thead">
        <span className="tr-col col-price">Price(USDT)</span>
        <span className="tr-col col-amount">Amount({symbol.replace("USDT", "")})</span>
        <span className="tr-col col-time">Time</span>
      </div>

      {/* List */}
      <div className="trades-list">
        {trades.length === 0 && <div className="text-center py-4 opacity-50">Waiting for data...</div>}
        
        {trades.map((trade, index) => {
           // Logic xác định màu sắc dựa trên 'side' từ JSON backend trả về
           const isBuy = trade.side === 'BUY'; 
           const colorClass = isBuy ? 'text-green' : 'text-red';
           
           return (
             <div key={index} className="trade-row">
               <span className={`tr-col col-price ${colorClass}`}>
                  {formatPrice(trade.price)}
               </span>
               <span className={`tr-col col-amount text-white`}>
                  {formatAmount(trade.amount)}
               </span>
               <span className={`tr-col col-time text-right`}>
                  {formatTime(trade.time)}
               </span>
             </div>
           );
        })}
      </div>
    </div>
  );
};

export default Trades;