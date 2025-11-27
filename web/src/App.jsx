import React from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';

// Import các trang (Đảm bảo đường dẫn đúng với cấu trúc thư mục của bạn)
import Login from './pages/Login'; 
import User_Dashboard from './pages/User_Dashboard'; 
import Admin_Dashboard from './pages/Admin_Dashboard'; 

// --- Component bảo vệ Route (Bắt buộc đăng nhập) ---
const PrivateRoute = ({ children }) => {
  const user = JSON.parse(localStorage.getItem("user"));
  return user ? children : <Navigate to="/login" />;
};

// --- Component bảo vệ Route Admin (Chỉ admin mới vào được) ---
const AdminRoute = ({ children }) => {
  const user = JSON.parse(localStorage.getItem("user"));
  // Nếu chưa đăng nhập -> Login
  if (!user) return <Navigate to="/login" />;
  // Nếu đăng nhập nhưng không phải admin -> Về Dashboard thường
  if (user.username !== "admin") return <Navigate to="/dashboard" />;
  
  return children;
};

function App() {
  const handleLogout = () => {
    localStorage.removeItem("user");
    window.location.href = "/login"; // Logout đơn giản
  };

  return (
    <BrowserRouter>
      <Routes>
        {/* Mặc định vào login */}
        <Route path="/" element={<Navigate to="/login" />} />
        
        <Route path="/login" element={<Login />} />

        {/* Route cho User thường (Trading) */}
        <Route 
          path="/dashboard" 
          element={
            <PrivateRoute>
              <User_Dashboard onLogout={handleLogout} />
            </PrivateRoute>
          } 
        />

        {/* Route cho Admin (Quản lý) */}
        <Route 
          path="/admin" 
          element={
            <AdminRoute>
              <Admin_Dashboard />
            </AdminRoute>
          } 
        />
        
        {/* Đường dẫn sai bất kỳ thì quay về login */}
        <Route path="*" element={<Navigate to="/login" />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;