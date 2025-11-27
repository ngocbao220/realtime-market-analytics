import React, { useEffect, useState, useRef } from 'react';
import '../styles/Orderbook.css'; // Đảm bảo tên file CSS khớp chính xác (hoa/thường)
import { MoreHorizontal, ArrowDown, ArrowUp } from 'lucide-react';

const OrderBook = ({ symbol = "BTCUSDT" }) => {
  const [bids, setBids] = useState([]);
  const [asks, setAsks] = useState([]);
  const [tickerPrice, setTickerPrice] = useState(0);
  const [priceTrend, setPriceTrend] = useState('equal'); // 'up', 'down', 'equal'

  const wsRef = useRef(null);
  const lastPriceRef = useRef(0);

  // --- HÀM XỬ LÝ DATA ---
  const processOrderBookData = (data) => {
    if (!Array.isArray(data)) return [];
    
    // Lấy 15 lệnh tốt nhất
    let slicedData = data.slice(0, 15);
    
    const normalized = slicedData.map(item => {
        // Backend có thể trả về [price, amount] hoặc {price, amount}
        const price = parseFloat(item.price || item[0]);
        const amount = parseFloat(item.amount || item[1]);
        return { 
            price, 
            amount, 
            total: price * amount 
        };
    });

    // Tính Max Volume để vẽ thanh Depth Bar
    const maxVol = Math.max(...normalized.map(i => i.amount), 0.0000001);

    return normalized.map(item => ({
        ...item,
        depthWidth: Math.min((item.amount / maxVol) * 100, 100)
    }));
  };

  useEffect(() => {
    // 1. Định nghĩa URL WebSocket chuẩn theo market.py
    // Lưu ý: Port 8000 là port backend FastAPI mặc định
    const WS_URL = `ws://localhost:8000/market/ws/orderbook/${symbol}?type=real&side=both`;
    
    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
        console.log(`Connected to Orderbook WS: ${symbol}`);
    };

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            
            // Backend trả về object: { bids: [...], asks: [...] }
            if (data) {
                // Asks: Cần reverse để giá thấp nhất (Best Ask) nằm dưới cùng (gần Ticker nhất)
                const newAsks = processOrderBookData(data.asks || []).reverse();
                const newBids = processOrderBookData(data.bids || []);

                setAsks(newAsks);
                setBids(newBids);

                // --- LOGIC TÍNH GIÁ TICKER REALTIME ---
                // Lấy trung bình cộng giữa Best Ask và Best Bid để làm giá tham chiếu tức thời
                if (newAsks.length > 0 && newBids.length > 0) {
                    const bestAsk = newAsks[newAsks.length - 1].price; // Giá bán thấp nhất
                    const bestBid = newBids[0].price;                  // Giá mua cao nhất
                    
                    // Tính giá thị trường ước lượng (Mid Price)
                    const estimatedPrice = (bestAsk + bestBid) / 2;
                    
                    const oldPrice = lastPriceRef.current;
                    if (oldPrice > 0) {
                        if (estimatedPrice > oldPrice) setPriceTrend('up');
                        else if (estimatedPrice < oldPrice) setPriceTrend('down');
                    }
                    
                    lastPriceRef.current = estimatedPrice;
                    setTickerPrice(estimatedPrice);
                }
            }
        } catch (err) {
            console.error("Error parsing WS message:", err);
        }
    };

    ws.onerror = (error) => {
        console.error("WebSocket Error:", error);
    };

    // Cleanup khi unmount
    return () => {
        if (wsRef.current) {
            wsRef.current.close();
        }
    };
  }, [symbol]);

  // --- FORMATTERS ---
  const formatPrice = (price) => {
    return new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(price);
  };

  const formatAmount = (num) => {
    // Số nhỏ hiển thị chi tiết, số lớn format chuẩn
    return num < 1 
        ? parseFloat(num).toFixed(5)
        : new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(num);
  };

  const formatTotal = (num) => {
    if (num >= 1000000) return (num / 1000000).toFixed(2) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(2) + 'K';
    return num.toFixed(2);
  };

  // Màu sắc Ticker giữa
  const trendColor = priceTrend === 'down' ? 'text-red' : 'text-green';

  return (
    <div className="orderbook-container">
      {/* Header */}
      <div className="ob-header">
        <span>Order Book</span>
        <div className="ob-icons">
             <div className="icon-group">
                <div className="w-3 h-3 border border-gray-500 rounded-sm hover:border-[#F0B90B] cursor-pointer"></div>
                <div className="w-3 h-3 bg-green-800 rounded-sm hover:border-[#F0B90B] cursor-pointer"></div>
                <div className="w-3 h-3 bg-red-800 rounded-sm hover:border-[#F0B90B] cursor-pointer"></div>
             </div>
             <MoreHorizontal size={16} className="cursor-pointer hover:text-white" />
        </div>
      </div>

      {/* Table Header */}
      <div className="ob-table-header text-xs text-gray-500 font-medium">
        <span className="th-item text-left">Price(USDT)</span>
        <span className="th-item text-right">Amount({symbol.replace('USDT','')})</span>
        <span className="th-item text-right">Total</span>
      </div>

      {/* --- ASKS (Bán - Đỏ) --- */}
      <div className="ob-list flex-1 flex-col justify-end">
        {asks.length === 0 && <div className="text-center py-4 text-xs opacity-50">Waiting for data...</div>}
        {asks.map((item, index) => (
          <div key={`ask-${index}`} className="ob-row">
            <div className="depth-bar bg-red" style={{ width: `${item.depthWidth}%` }}></div>
            <span className="td-item text-left text-red">{formatPrice(item.price)}</span>
            <span className="td-item text-right text-white opacity-90">{formatAmount(item.amount)}</span>
            <span className="td-item text-right text-white opacity-50">{formatTotal(item.total)}</span>
          </div>
        ))}
      </div>

      {/* --- CENTER TICKER --- */}
      <div className="ob-ticker">
         <span className={`ticker-price-large ${trendColor}`}>
            {formatPrice(tickerPrice)} 
         </span>
         
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
            <span className="td-item text-right text-white opacity-90">{formatAmount(item.amount)}</span>
            <span className="td-item text-right text-white opacity-50">{formatTotal(item.total)}</span>
          </div>
        ))}
      </div>
    </div>
  );
};

export default OrderBook;