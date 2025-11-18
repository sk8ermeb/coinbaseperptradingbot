
#from fastapi import APIRouter
from fastapi import APIRouter, Depends, HTTPException, Request, status, Body
from pydantic import BaseModel
from util import util
import secrets
import time
from fastapi.responses import JSONResponse

autil = util()
router = APIRouter(prefix="/api")

class Item(BaseModel):
    name: str
    value: int

async def require_session(request: Request):
    session = request.cookies.get("session")
    if autil.getconfig('anonymous') == 'true':
        return session
    if not session:
        raise HTTPException(status_code=401, detail="No session")
    sessions = autil.runselect("SELECT * FROM sessions WHERE sessionid=? LIMIT 1", (session,))
    if(len(sessions)> 0):
        return session
    else:
        raise HTTPException(status_code=401, detail="invalid session")

@router.post("/savesetting")
async def savesetting(session: str = Depends(require_session),
                     payload: dict = Body(...)):
    key = payload['settingkey']
    val = payload['settingval']
    autil.setkeyval(key, val)
    return {"session": session}


@router.post("/login")
async def login(data: dict, request: Request):
    guser = autil.getconfig('user')
    gpass = autil.getconfig('pass')
    if(data['username'] == guser and data['password'] == gpass):
        now_ts = int(time.time())
        delta = 365 * 24 * 60 * 60
        #delta = 20
        expire = now_ts + delta
        session_token = secrets.token_urlsafe(32)
        res = autil.runinsert("INSERT INTO sessions (sessionid, expiration) VALUES(?,?)", (session_token, expire))
    #also check to see if there is a matching username and password in the database in teh future
        response = JSONResponse({"message": "Login successful"})
        response.set_cookie(
            key="session",
            value=session_token,
            httponly=True,
            samesite="lax",
            path="/",
            secure=False,
            max_age=delta
            )
        return response
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Incorrect username or password",
        headers={"WWW-Authenticate": "Bearer"},  # optional, good for APIs
    )

