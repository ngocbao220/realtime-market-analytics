from fastapi import APIRouter, HTTPException
from ..schemas.models import UserRequest
from ..services import user_service

router = APIRouter(prefix="/user", tags=["User"])

@router.post("/create")
def create_user(user: UserRequest):
    # Dùng Pydantic model (UserRequest) thay vì dict -> Chuẩn hơn
    return user_service.create_new_user(user.username)

@router.get("/get/{user_id}")
def get_user(user_id: str):
    data = user_service.get_user_balance(user_id)
    if not data:
        raise HTTPException(status_code=404, detail="User not found")
    return data

@router.get("/get_all")
def get_all_users():
    return user_service.get_all_users_logic()

@router.delete("/delete/{user_id}")
def delete_user(user_id: str):
    result = user_service.delete_user_logic(user_id)
    
    if not result["success"]:
        # Trả về lỗi 400 nếu logic xóa thất bại (vd xóa admin)
        raise HTTPException(status_code=400, detail=result["message"])
        
    return result