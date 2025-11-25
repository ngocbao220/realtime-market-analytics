from fastapi import APIRouter, HTTPException
from schemas.models import UserRequest
from services import user_service

# Prefix chung là /users, không cần lặp lại chữ 'user' trong từng hàm
router = APIRouter(prefix="/users", tags=["Users"])

@router.post("")
def create_user(user: UserRequest):
    """Tạo người dùng mới"""
    return user_service.create_new_user(user.username)

@router.get("")
def get_all_users():
    """Lấy danh sách tất cả người dùng"""
    return user_service.get_all_users_logic()

@router.get("/{user_id}")
def get_user(user_id: str):
    """Lấy thông tin chi tiết user & số dư"""
    data = user_service.get_user_info(user_id)
    if not data:
        raise HTTPException(status_code=404, detail="User not found")
    return data

@router.delete("/{user_id}")
def delete_user(user_id: str):
    """Xóa người dùng"""
    result = user_service.delete_user_logic(user_id)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result