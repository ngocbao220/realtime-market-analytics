from pydantic import BaseModel, Field

class UserRequest(BaseModel):
    username: str

class PlaceOrderRequest(BaseModel):
    user_id: str
    symbol: str
    side: str = Field(..., description="buy or sell") # 'buy' hoặc 'sell'
    price: float
    amount: float