import React from "react";
import { BrowserRouter, Routes, Route, Navigate, useLocation } from "react-router-dom";
import Login from "./pages/Login";
import Header from "./components/Header";

function RequireAuth({ children }) {
  const user = localStorage.getItem("user");
  const location = useLocation();
  if (!user) {
    return <Navigate to="/login" state={{ from: location }} replace />;
  }
  return children;
}

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={<Login />} />
        <Route
          path="/*"
          element={
            <RequireAuth>
              <Header />
              {/* Thêm các route dashboard ở đây */}
            </RequireAuth>
          }
        />
      </Routes>
    </BrowserRouter>
  );
}

export default App;