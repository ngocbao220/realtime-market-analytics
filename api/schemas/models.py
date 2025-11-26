from pydantic import BaseModel, Field
from typing import Literal

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
    side: Literal["buy", "sell"]
    price: float = Field(..., gt=0, description="Giá đặt lệnh")
    amount: float = Field(..., gt=0, description="Số lượng coin")