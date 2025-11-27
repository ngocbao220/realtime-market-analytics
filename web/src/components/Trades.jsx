import React, { useEffect, useState, useRef } from 'react';
import '../styles/Trades.css';
import { api } from '../api/client';

const Trades = ({ symbol = "BTCUSDT" }) => {
  const [trades, setTrades] = useState([]);
  const intervalRef = useRef(null);

  // Hàm lấy dữ liệu
  const fetchTrades = async () => {
    try {
      const data = await api.getTrades(symbol, "real", 50); // Lấy 50 trade gần nhất
      if (Array.isArray(data)) {
        // API thường trả về mảng cũ -> mới hoặc mới -> cũ.
        // Binance UI luôn hiển thị mới nhất ở trên cùng (index 0).
        // Nếu API trả về trade cũ nhất ở đầu mảng, ta cần reverse.
        // Ở đây giả định API trả về đúng chuẩn (Mới nhất ở đầu hoặc cuối tùy server),
        // Ta sẽ kiểm tra timestamp để sort cho chắc chắn: Mới nhất (time lớn nhất) lên đầu.
        
        const sortedData = data.sort((a, b) => {
            // Lấy time từ trường time hoặc timestamp hoặc created_at
            const timeA = a.time || a.timestamp || 0;
            const timeB = b.time || b.timestamp || 0;
            return timeB - timeA; // Giảm dần (Mới nhất lên đầu)
        });

        setTrades(sortedData);
      }
    } catch (error) {
      console.error("Trades fetch error:", error);
    }
  };

  useEffect(() => {
    fetchTrades();
    // Refresh mỗi 2 giây
    intervalRef.current = setInterval(fetchTrades, 2000);
    return () => clearInterval(intervalRef.current);
  }, [symbol]);

  // --- FORMATTERS ---
  const formatPrice = (price) => {
    return new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(price);
  };

  const formatAmount = (num) => {
    return parseFloat(num).toFixed(5);
  };

  const formatTime = (ts) => {
    // Xử lý timestamp (API có thể trả về mili-giây hoặc giây)
    if (!ts) return "--:--:--";
    const date = new Date(ts); // Nếu API trả về giây thì nhân 1000: new Date(ts * 1000)
    return date.toLocaleTimeString('en-GB', { hour12: false }); // HH:MM:SS
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
        {trades.length === 0 && <div className="text-center py-4 opacity-50">Loading...</div>}
        
        {trades.map((trade, index) => {
           // Xác định màu sắc:
           // 1. Dựa vào trường 'side' ('buy'/'sell')
           // 2. Hoặc 'isBuyerMaker' (true = Sell/Red, false = Buy/Green)
           
           let isBuy = false;
           if (trade.side) {
               isBuy = trade.side.toLowerCase() === 'buy';
           } else if (trade.isBuyerMaker !== undefined) {
               isBuy = !trade.isBuyerMaker; // Binance logic
           }

           const colorClass = isBuy ? 'text-green' : 'text-red';
           
           // Fallback an toàn cho các trường dữ liệu
           const price = trade.price || trade.p || 0;
           const amount = trade.amount || trade.qty || trade.q || 0;
           const time = trade.time || trade.T || Date.now();

           return (
             <div key={index} className="trade-row">
               <span className={`tr-col col-price ${colorClass}`}>
                  {formatPrice(price)}
               </span>
               <span className={`tr-col col-amount text-white`}>
                  {formatAmount(amount)}
               </span>
               <span className={`tr-col col-time`}>
                  {formatTime(time)}
               </span>
             </div>
           );
        })}
      </div>
    </div>
  );
};

export default Trades;