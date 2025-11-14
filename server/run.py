# server.py
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os
from api import router   # ← this imports and attaches

app = FastAPI()
app.include_router(router)   # ← this line is required!

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WEB_DIR = BASE_DIR + "/web"
DATA_DIR = BASE_DIR+ "/data"

# Serve your HTML/JS/CSS
app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")

@app.get("/")
async def root():
    return FileResponse(os.path.join(WEB_DIR, "index.html"))

# Your API endpoints
#@app.get("/api/hello")
#async def hello():
#    return {"message": "Hello from FastAPI + Uvicorn"}

# Run with: python server.py   (or uvicorn server:app --reload)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
