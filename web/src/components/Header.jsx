import React, { useEffect, useState } from 'react';
import { Bot, Menu, Bell, User, Globe } from 'lucide-react'; 
import { api } from '../api/client'; // Đảm bảo đường dẫn đúng tới file api

const Header = () => {
  const [tickers, setTickers] = useState([]);

  useEffect(() => {
    // Nếu chưa có API backend chạy, header vẫn hiển thị nhưng không có giá chạy
    const fetchTickers = async () => {
      try {
        const data = await api.getTickers(); 
        if (Array.isArray(data)) {
           // Lấy vài coin mẫu để hiển thị
           setTickers(data.slice(0, 4));
        }
      } catch (e) { console.log("Chưa kết nối API"); }
    };
    fetchTickers();
  }, []);

  return (
    <header className="flex items-center justify-between px-6 h-16 bg-[#161a1e] border-b border-[#2B3139] text-[#EAECEF] font-sans">
      {/* Logo */}
      <div className="flex items-center gap-8">
        <div className="text-[#F0B90B] font-bold text-2xl cursor-pointer">BINANCE</div>
        
        {/* Menu Desktop */}
        <nav className="hidden lg:flex items-center gap-6 text-sm font-medium text-[#848E9C]">
          <a href="#" className="hover:text-[#F0B90B]">Markets</a>
          <a href="#" className="hover:text-[#F0B90B]">Trade</a>
        </nav>

        {/* Ticker chạy giá */}
        <div className="hidden xl:flex items-center gap-6 border-l border-[#2B3139] pl-6">
          {tickers.map((coin) => (
             <div key={coin.symbol} className="flex flex-col text-xs">
                <span className="font-bold text-[#848E9C]">{coin.symbol}</span>
                <span className="text-[#EAECEF]">{coin.price}</span>
             </div>
          ))}
          {tickers.length === 0 && <span className="text-xs text-gray-600">Loading Tickers...</span>}
        </div>
      </div>

      {/* Nút AI & User */}
      <div className="flex items-center gap-4">
        <button className="flex items-center gap-2 bg-gradient-to-r from-[#F0B90B] to-[#F8D33A] text-black px-4 py-1.5 rounded-full font-bold text-sm hover:shadow-lg transition-all">
          <Bot size={18} />
          <span>AI Helper</span>
        </button>
        <div className="w-8 h-8 rounded-full bg-[#2B3139] flex items-center justify-center cursor-pointer text-white">
            <User size={16} />
        </div>
      </div>
    </header>
  );
};

export default Header;