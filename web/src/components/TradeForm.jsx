import React, { useState, useEffect, useRef, useCallback } from 'react';
import '../styles/TradeSection.css';
import { api } from '../api/client';
import { useSymbolTicker } from '../contexts/TickerContext'; 
import { ChevronUp, ChevronDown } from 'lucide-react'; 

// --- 1. ĐƯA COMPONENT NÀY RA NGOÀI VÀ DÙNG REACT.MEMO ---
// React.memo giúp component này KHÔNG render lại trừ khi props (value) thay đổi
const CustomNumberInput = React.memo(({ value, onChange, onAdjust, placeholder, disabled, step }) => {
  return (
      <div className="custom-input-wrapper">
          <input 
              type="number" 
              className="trade-input no-spin" 
              value={value} 
              onChange={onChange}
              placeholder={placeholder}
              disabled={disabled}
              step={step}
          />
          {!disabled && (
            <div className="spin-controls">
                <button className="spin-btn up" onClick={() => onAdjust('inc')}>
                    <ChevronUp size={10} />
                </button>
                <button className="spin-btn down" onClick={() => onAdjust('dec')}>
                    <ChevronDown size={10} />
                </button>
            </div>
          )}
      </div>
  );
});

const TradeForm = ({ symbol = "BTCUSDT" }) => {
  const [orderType, setOrderType] = useState('Limit'); 
  
  const [buyPrice, setBuyPrice] = useState('');
  const [buyAmount, setBuyAmount] = useState('');
  const [sellPrice, setSellPrice] = useState('');
  const [sellAmount, setSellAmount] = useState('');
  
  const [user, setUser] = useState(null);
  const ticker = useSymbolTicker(symbol);
  
  const lastAutoFillRef = useRef(0); 

  useEffect(() => {
    const storedUser = localStorage.getItem("user");
    if (storedUser) setUser(JSON.parse(storedUser));
  }, []);

  // Reset form khi đổi Symbol
  useEffect(() => {
    setBuyPrice('');
    setSellPrice('');
    setBuyAmount('');
    setSellAmount('');
    lastAutoFillRef.current = 0; 
  }, [symbol]);

  // Logic tự động điền giá (15 phút/lần)
  useEffect(() => {
    if (ticker && ticker.close) {
        const now = Date.now();
        const FIFTEEN_MINUTES = 15 * 60 * 1000; 

        if (now - lastAutoFillRef.current >= FIFTEEN_MINUTES) {
            const closePrice = parseFloat(ticker.close);
            setBuyPrice(prev => prev === '' ? closePrice : prev);
            setSellPrice(prev => prev === '' ? closePrice : prev);
            lastAutoFillRef.current = now;
        }
    }
  }, [ticker, symbol]); 

  const isLoggedIn = !!user; 
  const baseAsset = symbol.replace("USDT", "");
  const quoteAsset = "USDT";

  // Dùng useCallback để tránh tạo lại function này mỗi lần render -> Tối ưu hiệu năng
  const handleAdjust = useCallback((setter, currentVal, step, type, precision) => {
      let val = parseFloat(currentVal) || 0;
      if (type === 'inc') val += step;
      else val -= step;
      val = Math.max(0, val);
      setter(val.toFixed(precision));
  }, []);

  const handlePlaceOrder = async (side) => {
    const price = side === 'BUY' ? buyPrice : sellPrice;
    const amount = side === 'BUY' ? buyAmount : sellAmount;
    
    if (!price || !amount) return alert("Vui lòng nhập giá và số lượng!");
    if (!user) return alert("Không tìm thấy thông tin người dùng!");

    try {
        const [success, message] = await api.placeOrder(
            user.id || user.user_id,
            symbol,
            side.toLowerCase(),
            price,
            amount
        );

        if (success) {
            alert(`✅ ${message}`);
            if (side === 'BUY') {
                setBuyPrice(''); setBuyAmount('');
            } else {
                setSellPrice(''); setSellAmount('');
            }
        } else {
            alert(`❌ Lỗi: ${message}`);
        }
    } catch (err) {
        console.error("Lỗi đặt lệnh:", err);
        alert("Có lỗi xảy ra khi kết nối server.");
    }
  };

  return (
    <div className="trade-form-wrapper">
      <div className="order-type-selector">
        <span className={`type-option ${orderType === 'Limit' ? 'active' : ''}`} onClick={() => setOrderType('Limit')}>Limit</span>
      </div>

      <div className="forms-container">
        {/* BUY FORM */}
        <div className="side-form">
          <div className="input-group">
            <span className="input-label">Price</span>
            <CustomNumberInput 
                value={buyPrice}
                onChange={(e) => setBuyPrice(e.target.value)}
                onAdjust={(type) => handleAdjust(setBuyPrice, buyPrice, 10, type, 2)} 
                disabled={orderType === 'Market'}
                placeholder={orderType === 'Market' ? "Market Price" : "0.00"}
                step={10}
            />
            <span className="input-suffix">{quoteAsset}</span>
          </div>
          <div className="input-group">
            <span className="input-label">Amount</span>
            <CustomNumberInput 
                value={buyAmount}
                onChange={(e) => setBuyAmount(e.target.value)}
                onAdjust={(type) => handleAdjust(setBuyAmount, buyAmount, 0.001, type, 3)}
                placeholder="0.00"
                step={0.001}
            />
            <span className="input-suffix">{baseAsset}</span>
          </div>
          {!isLoggedIn ? (
              <button className="action-btn btn-login" onClick={() => window.location.href='/login'}>Log In</button>
          ) : (
              <button className="action-btn btn-buy" onClick={() => handlePlaceOrder('BUY')}>
                  Buy {baseAsset}
              </button>
          )}
        </div>

        {/* SELL FORM */}
        <div className="side-form">
          <div className="input-group">
            <span className="input-label">Price</span>
            <CustomNumberInput 
                value={sellPrice}
                onChange={(e) => setSellPrice(e.target.value)}
                onAdjust={(type) => handleAdjust(setSellPrice, sellPrice, 10, type, 2)}
                disabled={orderType === 'Market'}
                placeholder={orderType === 'Market' ? "Market Price" : "0.00"}
                step={10}
            />
            <span className="input-suffix">{quoteAsset}</span>
          </div>
          <div className="input-group">
            <span className="input-label">Amount</span>
            <CustomNumberInput 
                value={sellAmount}
                onChange={(e) => setSellAmount(e.target.value)}
                onAdjust={(type) => handleAdjust(setSellAmount, sellAmount, 0.001, type, 3)}
                placeholder="0.00"
                step={0.001}
            />
            <span className="input-suffix">{baseAsset}</span>
          </div>
          {!isLoggedIn ? (
              <button className="action-btn btn-login" onClick={() => window.location.href='/login'}>Log In</button>
          ) : (
              <button className="action-btn btn-sell" onClick={() => handlePlaceOrder('SELL')}>
                  Sell {baseAsset}
              </button>
          )}
        </div>
      </div>
    </div>
  );
};

export default TradeForm;