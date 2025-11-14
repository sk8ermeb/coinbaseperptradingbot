
from fastapi import APIRouter

router = APIRouter(prefix="/api")

@router.get("/hello")
async def hello():
    return {"message": "Hello from FastAPI + Uvicorn"}

# add more endpoints here
