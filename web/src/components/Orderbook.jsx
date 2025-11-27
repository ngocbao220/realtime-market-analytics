import React, { useEffect, useState, useRef } from 'react';
import '../styles/OrderBook.css';
import { MoreHorizontal, ArrowDown, ArrowUp } from 'lucide-react';
import { api } from '../api/client';

const OrderBook = ({ symbol = "BTCUSDT" }) => {
  const [bids, setBids] = useState([]);
  const [asks, setAsks] = useState([]);
  const [tickerPrice, setTickerPrice] = useState(0);
  const [priceChange, setPriceChange] = useState(0);
  const [priceTrend, setPriceTrend] = useState('equal');

  const orderbookWsRef = useRef(null);
  const tickerWsRef = useRef(null);
  const lastPriceRef = useRef(0);

  // --- HÀM XỬ LÝ DATA ---
  const processOrderBookData = (data) => {
    if (!Array.isArray(data)) return [];
    
    let slicedData = data.slice(0, 15);
    
    const normalized = slicedData.map(item => {
        const price = parseFloat(item.price || item[0]);
        const amount = parseFloat(item.amount || item[1]);
        return { 
            price, 
            amount, 
            total: price * amount 
        };
    });

    const maxVol = Math.max(...normalized.map(i => i.amount), 0.0000001);

    return normalized.map(item => ({
        ...item,
        depthWidth: Math.min((item.amount / maxVol) * 100, 100)
    }));
  };

  // --- WEBSOCKET CHO ORDERBOOK ---
  useEffect(() => {
    const endpoint = `/market/ws/orderbook/${symbol}?type=real&side=both`;
    const socketUrl = api.getWebSocketUrl(endpoint);
    
    const ws = new WebSocket(socketUrl);
    orderbookWsRef.current = ws;

    ws.onopen = () => {
        console.log(`✅ Connected to Orderbook WS: ${symbol}`);
    };

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            
            if (data) {
                const newAsks = processOrderBookData(data.asks || []).slice(0, 15).reverse();
                const newBids = processOrderBookData(data.bids || []).slice(0, 15);

                
                setAsks(newAsks);
                setBids(newBids);
            }
        } catch (err) {
            console.error("Error parsing Orderbook WS message:", err);
        }
    };

    ws.onerror = (error) => {
        console.error("Orderbook WebSocket Error:", error);
    };

    return () => {
        if (orderbookWsRef.current) orderbookWsRef.current.close();
    };
  }, [symbol]);

  // --- WEBSOCKET CHO TICKER (GIÁ THẬT) ---
  useEffect(() => {
    const socketUrl = api.getWebSocketUrl('/ws/tickers');
    
    console.log("Connecting Ticker WS to:", socketUrl);
    
    const ws = new WebSocket(socketUrl);
    tickerWsRef.current = ws;

    ws.onopen = () => {
        console.log("✅ Connected to Ticker WebSocket");
    };

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            
            if (Array.isArray(data)) {
                // Tìm ticker của symbol hiện tại
                const currentTicker = data.find(ticker => ticker.symbol === symbol);
                
                if (currentTicker) {
                    const newPrice = parseFloat(currentTicker.price);
                    const change = parseFloat(currentTicker.change || 0);
                    
                    // Xác định trend dựa vào giá cũ
                    const oldPrice = lastPriceRef.current;
                    if (oldPrice > 0) {
                        if (newPrice > oldPrice) setPriceTrend('up');
                        else if (newPrice < oldPrice) setPriceTrend('down');
                        else setPriceTrend('equal');
                    }
                    
                    lastPriceRef.current = newPrice;
                    setTickerPrice(newPrice);
                    setPriceChange(change);
                }
            }
        } catch (err) {
            console.error("Error parsing Ticker WS message:", err);
        }
    };

    ws.onerror = (error) => {
        console.error("Ticker WebSocket Error:", error);
    };

    return () => {
        if (tickerWsRef.current) tickerWsRef.current.close();
    };
  }, [symbol]);

  // --- FORMATTERS ---
  const formatPrice = (price) => {
    return new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(price);
  };

  const formatAmount = (num) => {
    return num < 1 
        ? parseFloat(num).toFixed(5)
        : new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(num);
  };

  const formatTotal = (num) => {
    if (num >= 1000000) return (num / 1000000).toFixed(2) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(2) + 'K';
    return num.toFixed(2);
  };

  const trendColor = priceTrend === 'down' ? 'text-red' : 'text-green';
  const isPositiveChange = priceChange >= 0;

  return (
    <div className="orderbook-container">
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

      <div className="ob-table-header text-xs text-gray-500 font-medium">
        <span className="th-item text-left">Price(USDT)</span>
        <span className="th-item text-right">Amount({symbol.replace('USDT','')})</span>
        <span className="th-item text-right">Total</span>
      </div>

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
         
         <span className={`ticker-change ${isPositiveChange ? 'text-green' : 'text-red'}`}>
            {isPositiveChange ? '+' : ''}{priceChange.toFixed(2)}%
         </span>
      </div>

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