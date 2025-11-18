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


def getuserfromsession(sessionid):
    if(sessionid is None):
        return None
    sessions = myutil.runselect("SELECT * FROM sessions WHERE sessionid=? LIMIT 1", (sessionid,))
    if(len(sessions)> 0):
        if(sessions[0]['user'] is None):
            return myutil.getconfig("user")
        else:
            users = myutil.runselect("SELECT * FROM users WHERE id=?", (sessions[0]['user']))
            return users[0]['name']
    else:
        return None


@app.get("/")
async def root(request: Request):
    user = None
    resp = {"request":request}
    session_id = request.cookies.get("session")
    user = getuserfromsession(session_id)
    if user is not None:
        resp['user'] = user
    if user is None:
        anon = myutil.getconfig('anonymous')
        if anon == 'true':
            resp['anon'] = True
    return templates.TemplateResponse(
        "index.html", resp
    )


@app.get("/backtest")
async def root(request: Request):
    user = None
    resp = {"request":request}
    session_id = request.cookies.get("session")
    user = getuserfromsession(session_id)
    if user is not None:
        resp['user'] = user
    if user is None:
        anon = myutil.getconfig('anonymous')
        if anon == 'true':
            resp['anon'] = True
    return templates.TemplateResponse(
        "backtest.html", resp
    )

@app.get("/settings")
async def root(request: Request):
    user = None
    resp = {"request":request}
    session_id = request.cookies.get("session")
    user = getuserfromsession(session_id)
    if user is not None:
        resp['user'] = user
    if user is None:
        anon = myutil.getconfig('anonymous')
        if anon == 'true':
            resp['anon'] = True
    cbkey = myutil.getkeyval('cbkey')
    cbsecret = myutil.getkeyval('cbsecret')
    resp['cbkey'] = cbkey
    resp['cbsecret'] = cbsecret
    return templates.TemplateResponse(
        "settings.html", resp
    )

@app.get("/algorithms")
async def root(request: Request):
    user = None
    resp = {"request":request}
    session_id = request.cookies.get("session")
    user = getuserfromsession(session_id)
    if user is not None:
        resp['user'] = user
    if user is None:
        anon = myutil.getconfig('anonymous')
        if anon == 'true':
            resp['anon'] = True
    return templates.TemplateResponse(
        "algorithms.html", resp
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
