
#from fastapi import APIRouter
from fastapi import APIRouter, Depends, HTTPException, Request, status, Body, Query
from pydantic import BaseModel
from util import util
import secrets
import time
from fastapi.responses import JSONResponse
from simulate import Simulation

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

@router.get("/fetchscript")
async def savesetting(session: str = Depends(require_session),
                     scriptid: int = Query(..., description="Script ID")):
    res = autil.runselect("SELECT * FROM scripts WHERE id=?", (scriptid,))
    if(len(res) < 1):
        raise HTTPException(status_code=400, detail="bad request")
    response = JSONResponse({"name": res[0]['name'], 'script': res[0]['script']})
    return response

@router.delete("/deletescript/{scriptid}")
async def savesetting(session: str = Depends(require_session),
                      scriptid:int = -1):
    res = autil.runupdate("DELETE FROM scripts WHERE id=?", (scriptid,))
    if(res == -1):
        raise HTTPException(status_code=400, detail="bad request")
    response = JSONResponse({"scriptid": scriptid})
    return response

@router.post("/savescript")
async def savesetting(session: str = Depends(require_session),
                     payload: dict = Body(...)):
    scriptid = payload['scriptid']
    scriptname = payload['scriptname']
    script = payload['script']
    res = -1
    if(int(scriptid) == -1):
        res = autil.runinsert("INSERT INTO scripts (name, script) VALUES(?,?)", (scriptname, script))
        scriptid = res
    else:
        res = autil.runupdate("UPDATE scripts SET name=?, script=? WHERE id=?", (scriptname, script, scriptid))
    if(res == -1):
        raise HTTPException(status_code=400, detail="bad request")
    response = JSONResponse({"scriptid": scriptid})
    return response

@router.get("/fetchsim")
async def fetchsim(session: str = Depends(require_session),
                     simid: int = Query(..., description="Sim ID")):
    simidres = autil.runselect("SELECT * FROM exchangesim WHERE id=?", (simid,))
    if(len(simidres) < 1):
        raise HTTPException(status_code=400, detail="bad request")
    simidres = simidres[0]

    candles = autil.gethistoricledata(simidres['granularity'], simidres['pair'], simidres['start'], simidres['stop'])
    simevents = autil.runselect("SELECT * FROM simevent WHERE exchangesimid=?", (simid,))
    #i = 0
    #for candle in candles:
    #    candle['events'] = []
    #    candle['indicators'] = {}
    #    while i<len(simevents) and simevents[i]['candleid'] == candle['id']:
    #        candle['events'].append(simevents[i])
    #        i+=1
    simindicators = {}
    indnames = autil.runselect("SELECT DISTINCT indname FROM simindicator WHERE exchangesimid=? ORDER BY indname", (simid,))
    for indname in indnames:
        name = indname['indname']
        siminddata = autil.runselect("SELECT time, indval AS value FROM simindicator WHERE exchangesimid=? AND indname=? AND indval IS NOT NULL ORDER BY time", (simid,name))
        simindicators[name] = siminddata
    simassets = autil.runselect("SELECT * FROM simasset WHERE exchangesimid=?", (simid,))
    response = JSONResponse({'candles':candles, 'assets': simassets, 'indicators':simindicators, 'events':simevents})
    return response

@router.post("/startsim")
async def savesetting(session: str = Depends(require_session),
                     payload: dict = Body(...)):
    scriptid = payload['scriptid']
    start = payload['start']
    stop = payload['stop']
    cbkey = autil.getkeyval('cbkey')
    cbsecret = autil.getkeyval('cbsecret')
    if(cbkey is None or cbsecret is None or len(cbkey)==0 or len(cbsecret) == 0):
        raise HTTPException(status_code=400, detail="Missing Coinbase Credentials")

    #simstatus = autil.getkeyval("simstatus")
    #if(simstatus is None or simstatus != 'running'):
    #    autil.setkeyval("simstatus", "running")
    #else:
    #    raise HTTPException(status_code=400, detail="simrunning")
    mysim = Simulation(start, stop, scriptid)
    simid = mysim.simid
    rungood = mysim.runsim()
    if(mysim.good and rungood):
        response = JSONResponse({"simid": simid})
        return response
    else:
        simerr = autil.runselect("SELECT log, status FROM exchangesim WHERE id=?", (simid,))   
        raise HTTPException(status_code=400, detail=simerr[0]['log'])


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

