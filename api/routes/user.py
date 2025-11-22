from fastapi import APIRouter, HTTPException
from services.user_service import create_user, get_user, get_all_users, delete_user_service

router = APIRouter()

@router.post("/create")
def create_user_api(data: dict):
    return create_user(data["username"])

@router.get("/get/{user_id}")
def get_user_api(user_id: str):
    return get_user(user_id)

@router.get("/get_all")
def list_users_api():
    """API lấy danh sách toàn bộ user"""
    return get_all_users()

@router.delete("/delete/{user_id}")
def delete_user_api(user_id: str):
    result = delete_user_service(user_id)
    
    if not result["success"]:
        # Nếu lỗi logic (vd xóa admin hoặc ko tìm thấy), trả về 400 Bad Request
        raise HTTPException(status_code=400, detail=result["message"])
        
    return result
