import React from "react";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import User_Dashboard from "./pages/User_Dashboard"; // Import từ folder pages

function App() {
  return (
    <BrowserRouter> 
      <Routes>
        {/* Nếu vào trang chủ (/) thì tự nhảy sang /dashboard */}
        <Route path="/" element={<Navigate to="/user_dashboard" replace />} />
        
        {/* Route chính */}
        <Route path="/user_dashboard" element={<User_Dashboard />}/>
      </Routes>
    </BrowserRouter>
  )
}

export default App;