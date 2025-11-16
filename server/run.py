# server.py
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os
from api import router
from fastapi import Request, Response
from fastapi.templating import Jinja2Templates
from util import util
app = FastAPI()

#this links the api calls to the same site from api.py
app.include_router(router)  

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WEB_DIR = os.path.join(BASE_DIR, "web")
DATA_DIR = os.path.join(BASE_DIR,"data")
templates = Jinja2Templates(directory=WEB_DIR)

os.makedirs(DATA_DIR, exist_ok=True)

# Static file path
app.mount("/static", StaticFiles(directory=os.path.join(WEB_DIR, "static")), name="static")

myutil = util()

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


# Run with: python server.py   (or uvicorn server:app --reload)
if __name__ == "__main__":
    import uvicorn
    serverip = myutil.getconfig('serverip')
    serverport = myutil.getconfig('serverport')
    tls = myutil.getconfig('tls')
    if(tls.lower() == 'true'):
        print("Running with TLS enabled")
        cert, key = myutil.getservercert()
        uvicorn.run(app, host=serverip, port=int(serverport), ssl_keyfile=key, ssl_certfile=cert)
    else:
        print("Running without TLS enabled")
        uvicorn.run(app, host=serverip, port=int(serverport))
