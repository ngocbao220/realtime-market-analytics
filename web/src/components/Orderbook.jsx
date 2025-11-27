import React, { useEffect, useState, useRef } from 'react';
import '../styles/Orderbook.css';
import { MoreHorizontal, ArrowDown, ArrowUp } from 'lucide-react';
import { api } from '../api/client';

const OrderBook = ({ symbol = "BTCUSDT" }) => {
  const [bids, setBids] = useState([]);
  const [asks, setAsks] = useState([]);
  const [tickerPrice, setTickerPrice] = useState(0);
  
  // State lưu xu hướng giá: 'up' (tăng) hoặc 'down' (giảm) hoặc 'equal' (giữ nguyên)
  const [priceTrend, setPriceTrend] = useState('equal');

  // Dùng useRef để lưu giá cũ nhằm so sánh mà không bị lỗi stale state trong setInterval
  const lastPriceRef = useRef(0);
  const intervalRef = useRef(null);

  // --- HÀM XỬ LÝ DATA ---
  const processOrderBookData = (data) => {
    if (!Array.isArray(data)) return [];
    
    // Chỉ lấy 15 lệnh đầu tiên
    let slicedData = data.slice(0, 15);
    
    // Normalize dữ liệu về dạng object chuẩn { price, amount }
    const normalized = slicedData.map(item => {
        const price = parseFloat(item.price || item[0]);
        const amount = parseFloat(item.amount || item[1]);
        return { 
            price, 
            amount, 
            total: price * amount 
        };
    });

    // Tìm volume lớn nhất để vẽ thanh depth bar
    const maxVol = Math.max(...normalized.map(i => i.amount), 0.0000001);

    return normalized.map(item => ({
        ...item,
        depthWidth: Math.min((item.amount / maxVol) * 100, 100)
    }));
  };

  const fetchData = async () => {
    try {
      // Gọi song song API Orderbook và Ticker
      const [orderBookRes, tickersRes] = await Promise.all([
        api.getOrderbook(symbol),
        api.getTickers()
      ]);

      // 1. Xử lý Orderbook
      if (orderBookRes) {
        // Asks (Bán): Đảo ngược để giá thấp nhất (Best Ask) nằm dưới cùng
        setAsks(processOrderBookData(orderBookRes.asks || []).reverse()); 
        // Bids (Mua): Giữ nguyên để giá cao nhất (Best Bid) nằm trên cùng
        setBids(processOrderBookData(orderBookRes.bids || []));
      }

      // 2. Xử lý Ticker & Logic Mũi tên/Màu sắc
      if (Array.isArray(tickersRes)) {
        const found = tickersRes.find(t => t.symbol === symbol);
        if (found) {
            const newPrice = parseFloat(found.price);
            const oldPrice = lastPriceRef.current; // Lấy giá cũ từ Ref

            // LOGIC QUAN TRỌNG: So sánh giá mới và giá cũ
            if (oldPrice !== 0) { // Bỏ qua lần chạy đầu tiên
                if (newPrice > oldPrice) {
                    setPriceTrend('up');
                } else if (newPrice < oldPrice) {
                    setPriceTrend('down');
                }
                // Nếu bằng nhau thì giữ nguyên trend cũ
            }

            // Cập nhật lại Ref và State
            lastPriceRef.current = newPrice;
            setTickerPrice(newPrice);
        }
      }

    } catch (error) {
      console.error("Orderbook fetch error:", error);
    }
  };

  useEffect(() => {
    fetchData(); // Gọi ngay lần đầu
    
    // Auto refresh mỗi 3 giây
    intervalRef.current = setInterval(fetchData, 3000);

    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [symbol]);

  // --- FORMAT HELPER ---
  const formatPrice = (price) => {
    return new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(price);
  };

  const formatAmount = (num) => {
    return num < 1 
        ? num.toFixed(5) 
        : new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(num);
  };

  const formatTotal = (num) => {
    if (num >= 1000) return (num / 1000).toFixed(2) + 'K';
    return num.toFixed(2);
  };

  // Xác định class màu và icon dựa trên priceTrend
  const isUp = priceTrend === 'up' || priceTrend === 'equal'; // Mặc định xanh nếu chưa có biến động
  const trendColor = priceTrend === 'down' ? 'text-red' : 'text-green';

  return (
    <div className="orderbook-container">
      {/* Header */}
      <div className="ob-header">
        <span>Order Book</span>
        <div className="ob-icons">
             <div className="icon-group">
                <div className="w-4 h-4 border border-gray-600 rounded-sm cursor-pointer hover:border-[#F0B90B]"></div>
                <div className="w-4 h-4 border border-gray-600 rounded-sm bg-green-900 cursor-pointer hover:border-[#F0B90B]"></div>
                <div className="w-4 h-4 border border-gray-600 rounded-sm bg-red-900 cursor-pointer hover:border-[#F0B90B]"></div>
             </div>
             <MoreHorizontal size={16} className="cursor-pointer hover:text-white" />
        </div>
      </div>

      {/* Table Header */}
      <div className="ob-table-header">
        <span className="th-item text-left">Price(USDT)</span>
        <span className="th-item text-right">Amount({symbol.replace('USDT','')})</span>
        <span className="th-item text-right">Total</span>
      </div>

      {/* --- ASKS (Bán - Đỏ) --- */}
      <div className="ob-list flex-1 justify-end flex-col">
        {asks.length === 0 && <div className="text-center py-4 text-xs">Loading...</div>}
        {asks.map((item, index) => (
          <div key={`ask-${index}`} className="ob-row">
            <div className="depth-bar bg-red" style={{ width: `${item.depthWidth}%` }}></div>
            <span className="td-item text-left text-red">{formatPrice(item.price)}</span>
            <span className="td-item text-right text-white">{formatAmount(item.amount)}</span>
            <span className="td-item text-right text-white">{formatTotal(item.total)}</span>
          </div>
        ))}
      </div>

      {/* --- CENTER TICKER (GIÁ & MŨI TÊN) --- */}
      <div className="ob-ticker">
         <span className={`ticker-price-large ${trendColor}`}>
            {formatPrice(tickerPrice)} 
         </span>
         
         {/* Logic hiển thị mũi tên */}
         {priceTrend === 'down' ? (
             <ArrowDown size={16} className="text-red" />
         ) : (
             <ArrowUp size={16} className="text-green" />
         )}
         
         <span className="ticker-mark">${formatPrice(tickerPrice)}</span>
      </div>

      {/* --- BIDS (Mua - Xanh) --- */}
      <div className="ob-list flex-1">
        {bids.map((item, index) => (
          <div key={`bid-${index}`} className="ob-row">
            <div className="depth-bar bg-green" style={{ width: `${item.depthWidth}%` }}></div>
            <span className="td-item text-left text-green">{formatPrice(item.price)}</span>
            <span className="td-item text-right text-white">{formatAmount(item.amount)}</span>
            <span className="td-item text-right text-white">{formatTotal(item.total)}</span>
          </div>
        ))}
      </div>
    </div>
  );
};

export default OrderBook;