# server.py
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os
from api import router
from fastapi import Request, Response
from fastapi.templating import Jinja2Templates

app = FastAPI()

#this links the api calls to the same site from api.py
app.include_router(router)  

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WEB_DIR = BASE_DIR + "/web"
DATA_DIR = BASE_DIR+ "/data"
templates = Jinja2Templates(directory=WEB_DIR)

# Static file path
app.mount("/static", StaticFiles(directory=os.path.join(WEB_DIR, "static")), name="static")


@app.get("/")
async def root(request: Request):
    user = None
    session_id = request.cookies.get("sessionid")
    if session_id and is_valid_session(session_id):  # your check
        user = get_user_from_session(session_id)

    return templates.TemplateResponse(
        "index.html",
        {"request": request, "user": user}  # pass anything you want
    )
#@app.get("/")
#async def root():
#    return FileResponse(os.path.join(WEB_DIR, "index.html"))


# Run with: python server.py   (or uvicorn server:app --reload)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
