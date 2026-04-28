
#from fastapi import APIRouter
from fastapi import APIRouter, Depends, HTTPException, Request, status, Body, Query
from pydantic import BaseModel
from util import util
import secrets
import time
import json
import threading
from fastapi.responses import JSONResponse
from simulate import Simulation
import live as live_module

_running_sims: dict = {}

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
    pair = simidres['pair'].upper()+'-PERP-INTX'
    candles = autil.gethistoricledata(simidres['granularity'], pair, simidres['start'], simidres['stop'])
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

    # Build per-candle position state by replaying fill events in timestamp order.
    # Fill events carry usdcurr/cryptcurr/costbasis in their eventdata.
    leverage_str = autil.getkeyval(f'sim_{simid}_leverage')
    sim_leverage = float(leverage_str) if leverage_str else 10.0
    contract_size_str = autil.getkeyval(f'sim_{simid}_contract_size')
    sim_contract_size = float(contract_size_str) if contract_size_str else None
    base_increment_str = autil.getkeyval(f'sim_{simid}_base_increment')
    sim_base_increment = float(base_increment_str) if base_increment_str else None

    fill_events = sorted(
        [e for e in simevents if e['eventtype'].startswith('fill:')],
        key=lambda e: e['time']
    )
    running_usd = 10000.0
    running_contracts = 0.0
    running_costbasis = 0.0
    fill_idx = 0
    for candle in candles:
        ts = candle['timestamp']
        while fill_idx < len(fill_events) and fill_events[fill_idx]['time'] <= ts:
            try:
                edata = json.loads(fill_events[fill_idx]['eventdata'])
                running_usd = float(edata.get('usdcurr', running_usd))
                running_contracts = float(edata.get('cryptcurr', running_contracts))
                running_costbasis = float(edata.get('costbasis', running_costbasis))
            except Exception:
                pass
            fill_idx += 1
        close_price = float(candle['close'])
        locked = abs(running_contracts) * running_costbasis / sim_leverage
        if running_contracts > 0:
            upnl = (close_price - running_costbasis) * running_contracts
        elif running_contracts < 0:
            upnl = (running_costbasis - close_price) * abs(running_contracts)
        else:
            upnl = 0.0
        candle['sim_usd'] = round(running_usd, 2)
        candle['sim_contracts'] = round(running_contracts, 6)
        candle['sim_total_equity'] = round(running_usd + locked + upnl, 2)

    response = JSONResponse({
        'candles': candles, 'assets': simassets, 'indicators': simindicators,
        'events': simevents, 'log': simidres['log'], 'leverage': sim_leverage,
        'contract_size': sim_contract_size, 'base_increment': sim_base_increment,
    })
    return response

@router.get("/simhistory")
async def simhistory(session: str = Depends(require_session),
                     scriptid: int = Query(..., description="Script ID")):
    runs = autil.runselect(
        "SELECT id, runat, start, stop FROM exchangesim WHERE scriptid=? AND (status IS NULL OR status != -1) ORDER BY id DESC LIMIT 10",
        (scriptid,))
    return JSONResponse({"runs": runs})


@router.post("/startsim")
async def startsim(session: str = Depends(require_session),
                   payload: dict = Body(...)):
    scriptid = payload['scriptid']
    start = payload['start']
    stop = payload['stop']
    cbkey = autil.getkeyval('cbkey')
    cbsecret = autil.getkeyval('cbsecret')
    if cbkey is None or cbsecret is None or len(cbkey) == 0 or len(cbsecret) == 0:
        raise HTTPException(status_code=400, detail="Missing Coinbase Credentials")

    mysim = Simulation(start, stop, scriptid)
    simid = mysim.simid

    if not mysim.good:
        simerr = autil.runselect("SELECT log FROM exchangesim WHERE id=?", (simid,))
        raise HTTPException(status_code=400, detail=simerr[0]['log'])

    _running_sims[simid] = mysim

    def _run():
        try:
            rungood = mysim.runsim()
            if rungood:
                autil.setkeyval('simstartdt', str(start))
                autil.setkeyval('simstopdt', str(stop))
                all_runs = autil.runselect(
                    "SELECT id FROM exchangesim WHERE scriptid=? AND status=1 ORDER BY id DESC",
                    (scriptid,))
                if len(all_runs) > 10:
                    for old_run in all_runs[10:]:
                        old_id = old_run['id']
                        autil.runupdate("DELETE FROM simevent WHERE exchangesimid=?", (old_id,))
                        autil.runupdate("DELETE FROM simindicator WHERE exchangesimid=?", (old_id,))
                        autil.runupdate("DELETE FROM simasset WHERE exchangesimid=?", (old_id,))
                        autil.runupdate("DELETE FROM exchangesim WHERE id=?", (old_id,))
        finally:
            _running_sims.pop(simid, None)

    threading.Thread(target=_run, daemon=True).start()
    return JSONResponse({"simid": simid})


@router.get("/simstatus")
async def simstatus(session: str = Depends(require_session),
                    simid: int = Query(..., description="Sim ID")):
    rows = autil.runselect(
        "SELECT status, currenttick, totalticks, log FROM exchangesim WHERE id=?", (simid,))
    if not rows:
        raise HTTPException(status_code=400, detail="Sim not found")
    row = rows[0]
    status = row['status']
    current = row.get('currenttick') or 0
    total = row.get('totalticks') or 0
    pct = round(current / total * 100, 1) if total > 0 else 0
    return JSONResponse({
        'status': status,
        'current': current,
        'total': total,
        'pct': pct,
        'log': row.get('log') or '',
    })


@router.post("/stopsim")
async def stopsim(session: str = Depends(require_session),
                  payload: dict = Body(...)):
    simid = payload.get('simid')
    if simid is not None and simid in _running_sims:
        _running_sims[simid].cancelled = True
    return JSONResponse({"status": "stopping"})


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
        headers={"WWW-Authenticate": "Bearer"},
    )


# ------------------------------------------------------------------ Live trading routes

@router.post("/live/start")
async def live_start(session: str = Depends(require_session),
                     payload: dict = Body(...)):
    scriptid = int(payload.get('scriptid', -1))
    if scriptid < 0:
        raise HTTPException(status_code=400, detail="Invalid script ID")
    cbkey = autil.getkeyval('cbkey')
    cbsecret = autil.getkeyval('cbsecret')
    if not cbkey or not cbsecret:
        raise HTTPException(status_code=400, detail="Missing Coinbase credentials — add them in Settings")
    live_module.start_trader(scriptid)
    return JSONResponse({"status": "started", "scriptid": scriptid})


@router.post("/live/stop")
async def live_stop(session: str = Depends(require_session)):
    live_module.stop_trader()
    return JSONResponse({"status": "stopped"})


@router.get("/live/status")
async def live_status(session: str = Depends(require_session)):
    trader = live_module.get_trader()
    if trader is None:
        # Check metadata for last known state
        scriptid = autil.getkeyval('live_scriptid')
        return JSONResponse({
            'running': False, 'scriptid': int(scriptid) if scriptid else None,
            'pair': autil.getkeyval('live_pair') or 'btc',
            'granularity': autil.getkeyval('live_granularity') or 'ONE_HOUR',
            'usd': 0, 'realposition': 0, 'costbasis': 0,
            'close': 0, 'unrealized_pnl': 0, 'total_equity': 0,
            'leverage': 10, 'log': [],
        })
    return JSONResponse(trader.get_status())


@router.get("/live/candles")
async def live_candles(session: str = Depends(require_session),
                       pair: str = Query(None),
                       granularity: str = Query(None)):
    trader = live_module.get_trader()
    if trader and trader.running:
        use_pair = trader.pair
        use_gran = trader.granularity
    else:
        use_pair = pair or autil.getkeyval('live_pair') or 'btc'
        use_gran = granularity or autil.getkeyval('live_granularity') or 'ONE_HOUR'

    product_id = use_pair.upper() + '-PERP-INTX'
    gran_secs = live_module.GRAN_SECONDS.get(use_gran, 3600)
    import time as _time
    now = int(_time.time())
    start = now - 1000 * gran_secs
    candles = autil.gethistoricledata(use_gran, product_id, start, now)

    indicators = {}
    if trader and trader._ind_history:
        import math as _math
        for name, entries in trader._ind_history.items():
            indicators[name] = [
                {'time': e['time'], 'value': e['value']}
                for e in entries
                if not _math.isnan(e['value']) and not _math.isinf(e['value'])
            ]

    return JSONResponse({'candles': candles, 'indicators': indicators})


@router.get("/live/balance")
async def live_balance(session: str = Depends(require_session)):
    from coinbase_http import CoinbaseHTTP
    try:
        cb = CoinbaseHTTP()
        data = cb.get_balance_summary()
        bal = data.get('balance_summary', {})
        def _amount(key):
            v = bal.get(key, {})
            return float(v.get('value', 0) or 0)
        available = _amount('available_margin')
        buying_power = _amount('futures_buying_power')
        usd = available if available > 0 else buying_power
        total = _amount('total_usd_balance') or usd
        unrealized = _amount('unrealized_pnl')
        return JSONResponse({'usd': round(usd, 2), 'total_equity': round(total, 2), 'unrealized_pnl': round(unrealized, 2)})
    except Exception:
        return JSONResponse({'usd': 0, 'total_equity': 0, 'unrealized_pnl': 0})


@router.get("/live/price")
async def live_price(session: str = Depends(require_session)):
    from coinbase_http import CoinbaseHTTP
    trader = live_module.get_trader()
    use_pair = (trader.pair if trader else None) or autil.getkeyval('live_pair') or 'btc'
    product_id = use_pair.upper() + '-PERP-INTX'
    try:
        cb = CoinbaseHTTP()
        product = cb.get_product(product_id)
        bid = float(product.get('best_bid_price') or 0)
        ask = float(product.get('best_ask_price') or 0)
        if bid > 0 and ask > 0:
            price = (bid + ask) / 2
        elif bid > 0:
            price = bid
        elif ask > 0:
            price = ask
        else:
            price = float(product.get('price') or 0)
        return JSONResponse({'price': round(price, 2)})
    except Exception:
        pass
    return JSONResponse({'price': 0})


@router.get("/live/history")
async def live_history(session: str = Depends(require_session)):
    scriptid = autil.getkeyval('live_scriptid')
    if not scriptid:
        return JSONResponse({'events': []})
    events = autil.runselect(
        "SELECT * FROM liveevent WHERE scriptid=? ORDER BY time DESC LIMIT 200",
        (int(scriptid),))
    orders = autil.runselect(
        "SELECT * FROM liveorder WHERE scriptid=? ORDER BY time DESC LIMIT 100",
        (int(scriptid),))
    return JSONResponse({'events': events, 'orders': orders})


@router.get("/settings/ntfy")
async def settings_ntfy_get(session: str = Depends(require_session)):
    import ntfy_util
    return JSONResponse({'uuid': ntfy_util.get_uuid(), **ntfy_util.get_prefs()})


@router.post("/settings/ntfy/prefs")
async def settings_ntfy_prefs(session: str = Depends(require_session),
                               payload: dict = Body(...)):
    import ntfy_util
    ntfy_util.set_prefs(payload)
    return JSONResponse({'status': 'ok'})


@router.post("/ntfy/test")
async def ntfy_test(session: str = Depends(require_session)):
    import ntfy_util
    ok, msg = ntfy_util.send_test()
    if not ok:
        raise HTTPException(status_code=400, detail=msg)
    return JSONResponse({'status': 'sent'})


@router.get("/live/scriptgranularity")
async def live_script_granularity(session: str = Depends(require_session),
                                  scriptid: int = Query(...)):
    scripts = autil.runselect("SELECT script FROM scripts WHERE id=?", (scriptid,))
    if not scripts:
        raise HTTPException(status_code=404, detail="Script not found")
    # Quick parse: look for granularity = "..." assignment
    import re
    match = re.search(r'granularity\s*=\s*["\'](\w+)["\']', scripts[0]['script'])
    granularity = match.group(1) if match else 'ONE_HOUR'
    match_pair = re.search(r'pair\s*=\s*["\'](\w+)["\']', scripts[0]['script'])
    pair = match_pair.group(1) if match_pair else 'btc'
    return JSONResponse({'granularity': granularity, 'pair': pair})

