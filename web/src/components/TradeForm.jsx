import React, { useState } from 'react';
import '../styles/TradeSection.css';
import { api } from '../api/client';

const TradeForm = ({ symbol = "BTCUSDT" }) => {
  const [orderType, setOrderType] = useState('Limit'); 
  const [buyPrice, setBuyPrice] = useState('');
  const [buyAmount, setBuyAmount] = useState('');
  const [sellPrice, setSellPrice] = useState('');
  const [sellAmount, setSellAmount] = useState('');
  
  const isLoggedIn = false; 

  const baseAsset = symbol.replace("USDT", "");
  const quoteAsset = "USDT";

  const handlePlaceOrder = async (side) => {
    if (!isLoggedIn) return alert("Vui lòng đăng nhập!");
    
    const price = side === 'BUY' ? buyPrice : sellPrice;
    const amount = side === 'BUY' ? buyAmount : sellAmount;
    
    console.log(`Placing ${side} order: ${amount} ${baseAsset} @ ${price}`);
    // await api.placeOrder(symbol, side, price, amount);
  };

  return (
    <div className="trade-form-wrapper">

      {/* 2. Loại lệnh */}
      <div className="order-type-selector">
        <span className={`type-option ${orderType === 'Limit' ? 'active' : ''}`} onClick={() => setOrderType('Limit')}>Limit</span>
      </div>

      {/* 3. Form Container (2 Cột) */}
      <div className="forms-container">
        
        {/* --- CỘT TRÁI: MUA (BUY) --- */}
        <div className="side-form">
          <div className="input-group">
            <span className="input-label">Price</span>
            <input 
                type="number" 
                className="trade-input" 
                value={buyPrice} 
                onChange={(e) => setBuyPrice(e.target.value)}
                placeholder={orderType === 'Market' ? "Market Price" : "0.00"}
                disabled={orderType === 'Market'}
            />
            <span className="input-suffix">{quoteAsset}</span>
          </div>

          <div className="input-group">
            <span className="input-label">Amount</span>
            <input 
                type="number" 
                className="trade-input" 
                value={buyAmount}
                onChange={(e) => setBuyAmount(e.target.value)}
                placeholder="0.00"
            />
            <span className="input-suffix">{baseAsset}</span>
          </div>

          {!isLoggedIn ? (
              <button className="action-btn btn-login">Log In</button>
          ) : (
              <button className="action-btn btn-buy" onClick={() => handlePlaceOrder('BUY')}>
                  Buy {baseAsset}
              </button>
          )}
        </div>

        {/* --- CỘT PHẢI: BÁN (SELL) --- */}
        <div className="side-form">
          <div className="input-group">
            <span className="input-label">Price</span>
            <input 
                type="number" 
                className="trade-input" 
                value={sellPrice} 
                onChange={(e) => setSellPrice(e.target.value)}
                placeholder={orderType === 'Market' ? "Market Price" : "0.00"}
                disabled={orderType === 'Market'}
            />
            <span className="input-suffix">{quoteAsset}</span>
          </div>

          <div className="input-group">
            <span className="input-label">Amount</span>
            <input 
                type="number" 
                className="trade-input" 
                value={sellAmount}
                onChange={(e) => setSellAmount(e.target.value)}
                placeholder="0.00"
            />
            <span className="input-suffix">{baseAsset}</span>
          </div>

          {!isLoggedIn ? (
              <button className="action-btn btn-login">Log In</button>
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