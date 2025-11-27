import React, { useState } from 'react';
import '../styles/TradeSection.css'; // Import file CSS vừa tạo

const UserHistory = () => {
  const [activeTab, setActiveTab] = useState('OpenOrders');
  const isLoggedIn = false; 

  const tabs = [
    { id: 'OpenOrders', label: 'Open Orders(0)' },
    { id: 'OrderHistory', label: 'Order History' },
    { id: 'TradeHistory', label: 'Trade History' },
    { id: 'Funds', label: 'Funds' },
  ];

  return (
    <div className="user-history-container">
      {/* 1. Header Tabs */}
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

      {/* 2. Content Area */}
      <div className="history-content">
        {!isLoggedIn ? (
            <div className="empty-state">
               <div className="mb-2">Log In or Register Now to trade</div>
            </div>
        ) : (
            <div className="p-4 text-center text-gray-500 text-xs">
                No Data Available
            </div>
        )}
      </div>
    </div>
  );
};

export default UserHistory;