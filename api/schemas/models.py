from pydantic import BaseModel, Field

class UserRequest(BaseModel):
    username: str

class OrderRequest(BaseModel):
    user_id: str
    price: float
    amount: float
#CHo đặt lệnh
class PlaceOrderRequest(BaseModel):
    user_id: str
    symbol: str
    side: str = Field(..., description="buy hoặc sell")
    price: float = Field(..., gt=0, description="Giá đặt lệnh")
    amount: float = Field(..., gt=0, description="Số lượng coin")