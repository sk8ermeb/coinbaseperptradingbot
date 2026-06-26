
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
    # `pair` column now stores the full historical product_id (e.g. 'BTC-PERP-INTX').
    # Legacy rows held only the short code like 'btc' — auto-expand those.
    raw = (simidres['pair'] or '').strip()
    if raw and '-' not in raw:
        pair = raw.upper() + '-PERP-INTX'
    else:
        pair = raw
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
        # running_contracts is now a signed CONTRACT COUNT; convert to base-asset
        # exposure using contract_size before applying price to get notional/PnL.
        cs = sim_contract_size or 0
        notional = abs(running_contracts) * cs * running_costbasis
        locked = notional / sim_leverage if sim_leverage else 0
        if running_contracts > 0:
            upnl = (close_price - running_costbasis) * running_contracts * cs
        elif running_contracts < 0:
            upnl = (running_costbasis - close_price) * abs(running_contracts) * cs
        else:
            upnl = 0.0
        candle['sim_usd'] = round(running_usd, 2)
        candle['sim_contracts'] = int(running_contracts)
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
        "SELECT status, currenttick, totalticks, downloadticks, downloadtotal, log FROM exchangesim WHERE id=?", (simid,))
    if not rows:
        raise HTTPException(status_code=400, detail="Sim not found")
    row = rows[0]
    status = row['status']
    current = row.get('currenttick') or 0
    total = row.get('totalticks') or 0
    dl_cur = row.get('downloadticks') or 0
    dl_tot = row.get('downloadtotal') or 0
    pct      = round(current / total * 100, 1) if total > 0 else 0
    download_pct = round(dl_cur / dl_tot * 100, 1) if dl_tot > 0 else 100
    return JSONResponse({
        'status': status,
        'current': current,
        'total': total,
        'pct': pct,
        'download_current': dl_cur,
        'download_total': dl_tot,
        'download_pct': download_pct,
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
    # The trader's namespace only refreshes on candle close (every hour on
    # ONE_HOUR) and on WS FILLED/CANCELLED events. That leaves balance/position
    # stale between candles when the user places/cancels orders manually on
    # the Coinbase UI, or when intraday PnL/holds change. Refresh on every
    # status poll so the UI always reflects exchange truth. Uses the
    # lightweight balance+position path (skips order reconciliation, which is
    # heavy and has DB side effects — that stays on the candle-close path).
    if trader.pair:
        try:
            trader.refresh_balance_position(trader.pair, silent=True)
        except Exception:
            pass
    return JSONResponse(trader.get_status())


@router.get("/live/candles")
async def live_candles(session: str = Depends(require_session),
                       product_id: str = Query(None),
                       granularity: str = Query(None),
                       scriptid: int = Query(None)):
    trader = live_module.get_trader()
    if trader and trader.running:
        use_product = trader.pair
        use_gran = trader.granularity
    else:
        use_product = product_id or autil.getkeyval('live_pair') or ''
        use_gran = granularity or autil.getkeyval('live_granularity') or 'ONE_HOUR'

    if not use_product:
        return JSONResponse({'candles': [], 'indicators': {}})

    gran_secs = live_module.GRAN_SECONDS.get(use_gran, 3600)
    import time as _time
    now = int(_time.time())
    start = now - 1000 * gran_secs
    candles = autil.gethistoricledata(use_gran, use_product, start, now)

    indicators = {}
    if trader and trader._ind_history:
        import math as _math
        for name, entries in trader._ind_history.items():
            indicators[name] = [
                {'time': e['time'], 'value': e['value']}
                for e in entries
                if not _math.isnan(e['value']) and not _math.isinf(e['value'])
            ]

    # Events for the script being viewed. Prefer the explicit ?scriptid=
    # param from the frontend (matches the script dropdown the user is
    # actually looking at); fall back to the running trader's id only when
    # the frontend didn't say. Snap each event's time to its containing
    # candle and filter to chart-relevant categories.
    events = []
    if scriptid is None:
        scriptid = autil.getkeyval('live_scriptid')
    if scriptid and candles:
        win_start = int(candles[0]['timestamp'])
        win_end   = int(candles[-1]['timestamp']) + gran_secs
        rows = autil.runselect(
            "SELECT id, eventtype, eventdata, time FROM liveevent "
            "WHERE scriptid=? AND time >= ? AND time < ? "
            "AND (eventtype LIKE 'user:%' OR eventtype LIKE 'create:%' "
            "OR eventtype LIKE 'fill:%'  OR eventtype LIKE 'cancel:%') "
            "ORDER BY time, id",
            (int(scriptid), win_start, win_end))
        for r in rows:
            t = int(r['time'])
            events.append({
                'id': r['id'],
                'time': (t // gran_secs) * gran_secs,
                'eventtype': r['eventtype'],
                'eventdata': r['eventdata'],
            })

    # Open local liveorder rows for the viewed script. The chart draws its
    # stop/limit/activation horizontal lines from these so they appear the
    # moment the candle data loads, with no extra Coinbase round-trip.
    internal = []
    if scriptid:
        internal = autil.runselect(
            "SELECT * FROM liveorder WHERE scriptid=? AND status='open' "
            "ORDER BY time DESC, id DESC",
            (int(scriptid),))

    return JSONResponse({'candles': candles, 'indicators': indicators,
                         'events': events, 'internal': internal})


@router.get("/live/balance")
async def live_balance(session: str = Depends(require_session)):
    from coinbase_http import CoinbaseHTTP
    try:
        cb = CoinbaseHTTP()
        data = cb.get_balance_summary()
        bal = data.get('balance_summary', {})
        def _amount(key):
            v = bal.get(key, {})
            if isinstance(v, dict):
                return float(v.get('value', 0) or 0)
            return float(v or 0)
        total = _amount('total_usd_balance')
        initial_margin = _amount('initial_margin')
        hold = _amount('total_open_orders_hold_amount')
        unrealized = _amount('unrealized_pnl')
        daily_realized = _amount('daily_realized_pnl')
        available = _amount('available_margin')
        buying_power = _amount('futures_buying_power')
        # `futures_buying_power` matches Coinbase's UI "free margin" exactly.
        # `available_margin` empirically returns ~total equity on FCM/CDE
        # (e.g. $295 when free is $96) — not safe as a primary source.
        usd_computed = total - initial_margin - hold
        if buying_power > 0:
            usd = buying_power
        elif available > 0:
            usd = available
        else:
            usd = max(usd_computed, 0)
        # total_usd_balance is a static snapshot that doesn't include today's
        # realized P&L until end-of-day settlement, so add daily_realized_pnl
        # (signed) and unrealized_pnl to get the true mark-to-market equity.
        equity_base = total + daily_realized + unrealized if total else usd
        return JSONResponse({
            'usd': round(usd, 2),
            'total_equity': round(equity_base, 2),
            'unrealized_pnl': round(unrealized, 2),
            'daily_realized_pnl': round(daily_realized, 2),
            'initial_margin': round(initial_margin, 2),
            'open_orders_hold': round(hold, 2),
            'available_margin_raw': round(available, 2),
        })
    except Exception:
        return JSONResponse({'usd': 0, 'total_equity': 0, 'unrealized_pnl': 0})


@router.get("/live/price")
async def live_price(session: str = Depends(require_session)):
    from coinbase_http import CoinbaseHTTP
    import time as _time
    trader = live_module.get_trader()

    # Prefer the cache populated by the WS ticker push (primary) or the 30s
    # REST fallback. The backend is already getting sub-second price updates
    # on its own cadence; reading the cache here means the frontend doesn't
    # double up the request.
    if trader and trader.running and trader._last_price > 0:
        if _time.time() - trader._last_price_time < 10:
            return JSONResponse({'price': round(trader._last_price, 2),
                                 'source': 'cache'})

    # Fallback: no trader, or cache is stale. Fetch directly. (When trader
    # isn't running, update_trailing is a no-op via its internal guard, so
    # we don't bother calling it.)
    product_id = (trader.pair if trader else None) or autil.getkeyval('live_pair') or ''
    if not product_id:
        return JSONResponse({'price': 0})
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
        if trader and trader.running and price > 0:
            # Warm the cache so the next frontend poll skips the network.
            trader._last_price = price
            trader._last_price_time = _time.time()
            trader.update_trailing(price)
        return JSONResponse({'price': round(price, 2), 'source': 'fetch'})
    except Exception:
        pass
    return JSONResponse({'price': 0})


@router.get("/live/account")
async def live_account(session: str = Depends(require_session),
                       product_id: str = Query(...)):
    """Unified account snapshot for a single product, pulled live from Coinbase.
    Used by the trading page on load and on product change so values always
    reflect the exchange state — not local calculations that depend on
    contract_size / leverage assumptions that drift from reality."""
    from coinbase_http import CoinbaseHTTP, KNOWN_CONTRACT_SIZES
    cb = CoinbaseHTTP()
    out = {
        'product_id': product_id,
        'usd': 0.0, 'total_equity': 0.0, 'unrealized_pnl': 0.0,
        'initial_margin': 0.0, 'open_orders_hold': 0.0,
        'realposition': 0.0, 'costbasis': 0.0,
        'mark_price': 0.0, 'contract_size': None, 'base_increment': None,
        'price_increment': None,
        'max_leverage': None, 'product_venue': '', 'base_currency': '',
        'errors': {},
    }

    # Balance summary — equity/PnL/margin live numbers
    try:
        bal = (cb.get_balance_summary() or {}).get('balance_summary', {}) or {}
        def _amt(key):
            v = bal.get(key, {})
            return float(v.get('value', 0) or 0) if isinstance(v, dict) else float(v or 0)
        total = _amt('total_usd_balance')
        initial_margin = _amt('initial_margin')
        hold = _amt('total_open_orders_hold_amount')
        unrealized = _amt('unrealized_pnl')
        daily_realized = _amt('daily_realized_pnl')
        available = _amt('available_margin')
        buying_power = _amt('futures_buying_power')
        usd_computed = total - initial_margin - hold
        # `futures_buying_power` is the actual "free margin" Coinbase's UI shows.
        # `available_margin` empirically returns ~total equity on FCM/CDE
        # (e.g. $295 when free is $96) — not safe as a primary source.
        if buying_power > 0:
            out['usd'] = round(buying_power, 2)
        elif available > 0:
            out['usd'] = round(available, 2)
        else:
            out['usd'] = round(max(usd_computed, 0), 2)
        # total_usd_balance is a static snapshot that excludes today's realized
        # P&L until end-of-day settlement, so include daily_realized_pnl and
        # unrealized_pnl to get the true mark-to-market equity.
        out['total_equity']        = round(total + daily_realized + unrealized, 2)
        out['unrealized_pnl']      = round(unrealized, 2)
        out['daily_realized_pnl']  = round(daily_realized, 2)
        out['initial_margin']      = round(initial_margin, 2)
        out['open_orders_hold']    = round(hold, 2)
    except Exception as e:
        out['errors']['balance'] = str(e)

    # Position — contracts + average entry. No position → zeros (not an error).
    try:
        pos = (cb.get_position(product_id) or {}).get('position') or {}
        contracts = float(pos.get('number_of_contracts', 0) or 0)
        if (pos.get('side') or '').upper() == 'SHORT':
            contracts = -contracts
        out['realposition'] = contracts
        out['costbasis']    = round(float(pos.get('avg_entry_price', 0) or 0), 2)
    except Exception as e:
        out['errors']['position'] = str(e)

    # Product details — mark price, contract size, venue, max leverage
    try:
        product = cb.get_product(product_id) or {}
        try:
            if isinstance(product, dict) and product.get('product_id'):
                autil.upsert_futures_product(product)
        except Exception:
            pass
        bid = float(product.get('best_bid_price') or 0)
        ask = float(product.get('best_ask_price') or 0)
        if bid > 0 and ask > 0: mark = (bid + ask) / 2
        elif bid > 0:           mark = bid
        elif ask > 0:           mark = ask
        else:                   mark = float(product.get('price') or 0)
        out['mark_price'] = round(mark, 2)
        out['base_increment'] = float(product.get('base_increment') or 0) or None
        # `price_increment` is the actual price tick (e.g. $5 for CDE BTC futures);
        # `quote_increment` is just USD precision and is misleading on coarse-tick products.
        out['price_increment'] = (float(product.get('price_increment') or 0) or
                                  float(product.get('quote_increment') or 0) or None)
        out['product_venue']  = (product.get('product_venue') or '').upper()
        fpd = product.get('future_product_details') or {}
        # For futures, base_currency_id / base_name are usually blank on the
        # product payload — the root unit (e.g. "BTC") lives on
        # future_product_details.contract_root_unit instead.
        out['base_currency'] = (
            product.get('base_currency_id') or product.get('base_name') or
            fpd.get('contract_root_unit') or ''
        ).upper()
        api_cs = float(fpd.get('contract_size') or 0) or None
        # INTX contract_size from the API is unreliable post-merger; prefer hardcoded.
        known = KNOWN_CONTRACT_SIZES.get(product_id)
        if out['product_venue'] == 'INTX' and known is not None:
            out['contract_size'] = known
        else:
            out['contract_size'] = api_cs or known
        pd = fpd.get('perpetual_details') or {}
        ml = float(pd.get('max_leverage') or 0)
        if ml > 0:
            out['max_leverage'] = ml
    except Exception as e:
        out['errors']['product'] = str(e)

    # Final fallback for Base: if the product fetch didn't populate it, use
    # the cached row's contract_root_unit so the field never stays blank when
    # the product is known locally.
    if not out['base_currency']:
        try:
            row = autil.get_futures_product(product_id) or {}
            root = (row.get('contract_root_unit') or '').upper()
            if root:
                out['base_currency'] = root
        except Exception:
            pass

    return JSONResponse(out)


@router.get("/live/open_orders")
async def live_open_orders(session: str = Depends(require_session)):
    """Live count + detail of OPEN orders on Coinbase for derivatives products,
    plus our internal liveorder rows so the modal can surface trail state
    (trail %, activation, peak) that Coinbase doesn't track."""
    from coinbase_http import CoinbaseHTTP
    scriptid = autil.getkeyval('live_scriptid')
    internal = []
    if scriptid:
        internal = autil.runselect(
            "SELECT * FROM liveorder WHERE scriptid=? AND status='open' "
            "ORDER BY time DESC, id DESC",
            (int(scriptid),))
    try:
        cb = CoinbaseHTTP()
        data = cb.list_orders(order_status=['OPEN'], product_type='FUTURE', limit=100)
        orders = data.get('orders', []) or []
        return JSONResponse({'orders': orders, 'count': len(orders), 'internal': internal})
    except Exception as e:
        return JSONResponse({'orders': [], 'count': None, 'error': str(e),
                             'internal': internal})


@router.get("/live/history")
async def live_history(session: str = Depends(require_session), page: int = Query(0)):
    scriptid = autil.getkeyval('live_scriptid')
    if not scriptid:
        return JSONResponse({'events': [], 'orders': [], 'page': page})
    offset = page * 300
    # id is autoincrement on insert, so it breaks `time` ties in actual
    # insertion order — needed because cancel/place/WS events fire from
    # different threads within the same second.
    events = autil.runselect(
        "SELECT * FROM liveevent WHERE scriptid=? ORDER BY time DESC, id DESC LIMIT 300 OFFSET ?",
        (int(scriptid), offset))
    orders = autil.runselect(
        "SELECT * FROM liveorder WHERE scriptid=? ORDER BY time DESC, id DESC LIMIT 100",
        (int(scriptid),))
    return JSONResponse({'events': events, 'orders': orders, 'page': page})


# ------------------------------------------------------------------ financials helpers
# Shared by /live/financials (display) and /live/financials/fix (reconcile) so the
# two always agree on how fills pair into round-trips.

_FILL_EXIT_TYPES = ('fill:Exit', 'fill:ExitLong', 'fill:ExitShort', 'fill:Liquidation')


def _normalize_fill_events(rows):
    """Normalize raw liveevent fill rows to a common shape across both log
    schemas: the rich suffixed schema (tradetype/amount/average_filled_price/
    total_fees/coinbase_order_id) and the lean reconciliation schema
    (side/filled/avg_price/order_id, no fees). Carries the DB row id so callers
    can write corrections back to the source event."""
    norm = []
    for r in rows:
        et = r['eventtype']
        try:
            d = json.loads(r['eventdata']) if r['eventdata'] else {}
        except Exception:
            d = {}
        tt = d.get('tradetype') or (et.split(':', 1)[1] if ':' in et else '')
        side = (d.get('side') or '').upper()  # reconciliation schema
        # Side: long-opening (Buy) vs short-opening (Sell), independent of
        # whether this fill is an entry or an exit.
        if tt in ('Buy', 'EnterLong') or side == 'BUY':
            sidetag = 'Buy'
        elif tt in ('Sell', 'EnterShort') or side == 'SELL':
            sidetag = 'Sell'
        else:
            sidetag = ''  # bare Exit/Liquidation — direction resolved from position
        norm.append({
            'id': r['id'],
            'is_exit': (et in _FILL_EXIT_TYPES),
            'is_liq': et.startswith('fill:Liquidation'),
            'sidetag': sidetag,
            'tt': tt,
            'oid': d.get('coinbase_order_id') or d.get('order_id') or '',
            'time': int(r['time']),
            'contracts': abs(float(d.get('amount', d.get('filled', 0)) or 0)),
            'price': float(d.get('average_filled_price', d.get('avg_price', 0)) or 0),
            'fees': float(d.get('total_fees', 0) or 0),
            'paf': d.get('ProfitAfterFees'),
            'has_tt': bool(d.get('tradetype')),
        })
    return norm


def _dedupe_fills(norm):
    """Collapse fills logged twice (same order id under both schemas), keeping
    the richer record (the one carrying a tradetype), then sort by time."""
    seen = {}  # order id -> index into deduped
    deduped = []
    for f in norm:
        oid = f['oid']
        if oid and oid in seen:
            idx = seen[oid]
            if f['has_tt'] and not deduped[idx]['has_tt']:
                deduped[idx] = f  # upgrade to the richer copy
            continue
        if oid:
            seen[oid] = len(deduped)
        deduped.append(f)
    deduped.sort(key=lambda f: f['time'])
    return deduped


def _pair_fills(deduped):
    """Position-aware walk: fills opening/adding to a position accumulate as
    entry legs; the first opposite-side (or explicit exit) fill closes it and
    emits one round-trip. A still-open trailing position is not emitted."""
    completed = []

    def _emit(pending, exitf):
        entry_contracts = sum(p['contracts'] for p in pending)
        entry_fees = sum(p['fees'] for p in pending)
        notional = sum(p['contracts'] * p['price'] for p in pending)
        entry_price = (notional / entry_contracts) if entry_contracts > 0 else 0.0
        entry_dir = pending[0]['sidetag'] if pending else None
        entry_time = pending[0]['time'] if pending else None
        completed.append({
            'entry_dir': entry_dir,
            'entry_time': entry_time,
            'entry_contracts': entry_contracts,
            'entry_price': entry_price,
            'exit_dir': (exitf['sidetag'] or
                         (None if entry_dir is None else
                          ('Sell' if entry_dir == 'Buy' else 'Buy'))),
            'exit_time': exitf['time'],
            'exit_contracts': exitf['contracts'],
            'exit_price': exitf['price'],
            'total_fees': entry_fees + exitf['fees'],
            'total_pnl': (float(exitf['paf']) if exitf['paf'] is not None else None),
            'liquidation': exitf['is_liq'],
            'exit_id': exitf['id'],
            'entry_ids': [p['id'] for p in pending],
        })

    pending = []
    for f in deduped:
        opp = (pending and f['sidetag'] and f['sidetag'] != pending[0]['sidetag'])
        if f['is_exit'] or opp:
            _emit(pending, f)
            pending = []
        else:
            pending.append(f)
    return completed


def _load_round_trips(scriptid):
    """Full pipeline: load fill events for a script, normalize, dedupe, pair."""
    rows = autil.runselect(
        "SELECT id, eventtype, eventdata, time FROM liveevent WHERE scriptid=? AND "
        "(eventtype LIKE 'fill:%' OR eventtype='fill') ORDER BY time ASC, id ASC",
        (int(scriptid),))
    return _pair_fills(_dedupe_fills(_normalize_fill_events(rows)))


@router.get("/live/financials")
async def live_financials(session: str = Depends(require_session),
                          scriptid: int = Query(None),
                          page: int = Query(0)):
    """Paired entry→exit round-trips for the Financials tab, most-recent-first,
    10 rows per page. PnL comes from the exit fill's stored ProfitAfterFees
    (already net of BOTH entry and exit fees); total fees is the sum of every
    entry-leg fee plus the exit fee. See the helpers above for the pairing."""
    if scriptid is None:
        scriptid = autil.getkeyval('live_scriptid')
    if not scriptid:
        return JSONResponse({'rows': [], 'page': page, 'has_more': False})

    PER_PAGE = 10
    completed = _load_round_trips(scriptid)
    completed.reverse()  # most recent first
    start = page * PER_PAGE
    page_rows = completed[start:start + PER_PAGE]
    has_more = len(completed) > start + PER_PAGE
    return JSONResponse({'rows': page_rows, 'page': page, 'has_more': has_more,
                         'total': len(completed)})


@router.post("/live/financials/fix")
async def live_financials_fix(session: str = Depends(require_session),
                              scriptid: int = Query(None)):
    """Reconcile the local fill log against Coinbase's authoritative fills and
    repair gaps in place (the user opted for apply-immediately):

      1. Pull every Coinbase fill (paginated) for each product the script traded,
         bounded to the script's event time window, and aggregate per order id
         (size-weighted price, summed commission, side, earliest trade time).
      2. Self-calibrate the size→contracts ratio from any order present in BOTH
         the local log (known contract amount) and Coinbase (raw size) — avoids
         guessing the product's size unit.
      3. For each local fill event, fill in any missing price / contracts / fees
         from the matching Coinbase order (never overwrites good values, only
         fills zeros/blanks), and stamp it reconciled.
      4. For Coinbase orders with no local fill event at all (entries dropped
         during the failure/retry handling), insert a synthetic fill:<side>
         event at the Coinbase trade time so the round-trip is complete.
      5. Recompute ProfitAfterFees on every exit event from the now-complete
         entry/exit prices, contracts, fees, and the product contract_size.

    Idempotent: re-running matches the synthetic events by order id and only
    re-applies the same authoritative values."""
    from coinbase_http import CoinbaseHTTP, KNOWN_CONTRACT_SIZES
    from datetime import datetime, timezone

    if scriptid is None:
        scriptid = autil.getkeyval('live_scriptid')
    if not scriptid:
        return JSONResponse({'ok': False, 'error': 'No script selected.'})
    scriptid = int(scriptid)

    summary = {'ok': True, 'products': [], 'enriched': 0, 'inserted': 0,
               'pnl_updated': 0, 'cb_fills': 0, 'errors': []}

    # ---- Source events: discover products + time window --------------------
    rows = autil.runselect(
        "SELECT id, eventtype, eventdata, time FROM liveevent WHERE scriptid=? AND "
        "(eventtype LIKE 'fill:%' OR eventtype='fill') ORDER BY time ASC, id ASC",
        (scriptid,))
    if not rows:
        return JSONResponse({'ok': True, 'message': 'No fills to reconcile.',
                             **summary})

    products = set()
    times = []
    for r in rows:
        times.append(int(r['time']))
        try:
            d = json.loads(r['eventdata']) if r['eventdata'] else {}
        except Exception:
            d = {}
        if d.get('product_id'):
            products.add(d['product_id'])
    cfg_pair = autil.getkeyval('live_pair')
    if cfg_pair:
        products.add(cfg_pair)
    if not products:
        return JSONResponse({'ok': False, 'error': 'Could not determine product id.'})
    # 2-day pad around the window so boundary fills are captured.
    start_ts = min(times) - 2 * 86400
    end_ts = max(times) + 2 * 86400

    def _iso(ts):
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    def _parse_ts(s):
        if not s:
            return None
        try:
            return int(datetime.fromisoformat(s.replace('Z', '+00:00')).timestamp())
        except Exception:
            return None

    cb = CoinbaseHTTP()

    # ---- 1. Pull + aggregate Coinbase fills per order id -------------------
    cb_orders = {}  # order_id -> {side, size, notional, commission, time}
    for pid in products:
        summary['products'].append(pid)
        cursor = None
        pages = 0
        try:
            while pages < 100:  # hard cap against runaway pagination
                resp = cb.list_fills(product_id=pid,
                                     start_sequence_timestamp=_iso(start_ts),
                                     end_sequence_timestamp=_iso(end_ts),
                                     limit=100, cursor=cursor) or {}
                fills = resp.get('fills', []) or []
                for fl in fills:
                    oid = fl.get('order_id') or ''
                    if not oid:
                        continue
                    size = abs(float(fl.get('size', 0) or 0))
                    price = float(fl.get('price', 0) or 0)
                    comm = float(fl.get('commission', 0) or 0)
                    side = (fl.get('side') or '').upper().replace('FUTURES_ORDER_SIDE_', '')
                    t = _parse_ts(fl.get('trade_time') or fl.get('sequence_timestamp'))
                    agg = cb_orders.setdefault(oid, {
                        'side': side, 'size': 0.0, 'notional': 0.0,
                        'commission': 0.0, 'time': t})
                    agg['size'] += size
                    agg['notional'] += size * price
                    agg['commission'] += comm
                    if side:
                        agg['side'] = side
                    if t is not None and (agg['time'] is None or t < agg['time']):
                        agg['time'] = t
                    summary['cb_fills'] += 1
                cursor = resp.get('cursor')
                pages += 1
                if not cursor or not fills:
                    break
        except Exception as e:
            summary['errors'].append(f'list_fills({pid}): {e}')

    if not cb_orders:
        summary['message'] = ('No Coinbase fills returned for the window — '
                              'nothing to reconcile.')
        return JSONResponse(summary)

    for agg in cb_orders.values():
        agg['avg_price'] = (agg['notional'] / agg['size']) if agg['size'] > 0 else 0.0

    # ---- 2. Self-calibrate the size→contracts ratio -----------------------
    norm = _normalize_fill_events(rows)
    local_by_oid = {f['oid']: f for f in norm if f['oid']}
    ratio = 1.0
    for oid, lf in local_by_oid.items():
        agg = cb_orders.get(oid)
        if agg and agg['size'] > 0 and lf['contracts'] > 0:
            ratio = lf['contracts'] / agg['size']
            break

    def _contracts(size):
        return size * ratio

    # ---- contract_size per product (for PnL $ value) ----------------------
    contract_size = None
    for pid in products:
        try:
            prod = cb.get_product(pid) or {}
            fd = prod.get('future_product_details') or {}
            cs = float(fd.get('contract_size', 0) or 0)
            venue = (prod.get('product_venue') or '').upper()
            if venue == 'INTX' and KNOWN_CONTRACT_SIZES.get(pid):
                cs = KNOWN_CONTRACT_SIZES[pid]
            if cs > 0:
                contract_size = cs
                break
        except Exception as e:
            summary['errors'].append(f'get_product({pid}): {e}')
    if not contract_size:
        contract_size = 1.0

    # ---- 3. Enrich existing local fill events -----------------------------
    for lf in norm:
        agg = cb_orders.get(lf['oid'])
        if not agg:
            continue
        try:
            erows = autil.runselect(
                "SELECT eventdata FROM liveevent WHERE id=?", (lf['id'],))
            d = json.loads(erows[0]['eventdata']) if erows and erows[0]['eventdata'] else {}
        except Exception:
            d = {}
        changed = False
        cb_contracts = _contracts(agg['size'])
        # Only fill blanks/zeros — never clobber values the bot logged correctly.
        if not float(d.get('average_filled_price', 0) or 0) and agg['avg_price']:
            d['average_filled_price'] = round(agg['avg_price'], 8); changed = True
        if not float(d.get('amount', 0) or 0) and cb_contracts:
            d['amount'] = round(cb_contracts, 8); changed = True
        if not float(d.get('total_fees', 0) or 0) and agg['commission']:
            d['total_fees'] = round(agg['commission'], 8); changed = True
        if changed:
            d['reconciled'] = True
            autil.runupdate("UPDATE liveevent SET eventdata=? WHERE id=?",
                            (json.dumps(d), lf['id']))
            summary['enriched'] += 1

    # ---- 4. Insert synthetic events for orders missing locally -------------
    # Guard against duplicates when a real fill was logged under a different
    # order id than Coinbase reports: skip if a local fill of the same side
    # sits within 3 minutes of this Coinbase trade (real trades here are hours
    # apart, so this only catches the same fill, never a distinct one).
    local_fills_by_side = {}
    for lf in norm:
        if lf['sidetag']:
            local_fills_by_side.setdefault(lf['sidetag'], []).append(lf['time'])

    for oid, agg in cb_orders.items():
        if oid in local_by_oid:
            continue
        side = agg['side']
        if side not in ('BUY', 'SELL'):
            continue
        sidetag = 'Buy' if side == 'BUY' else 'Sell'
        at = agg['time'] or int(min(times))
        if any(abs(at - t) < 180 for t in local_fills_by_side.get(sidetag, [])):
            continue
        et = 'fill:Buy' if side == 'BUY' else 'fill:Sell'
        d = {
            'coinbase_order_id': oid,
            'tradetype': 'Buy' if side == 'BUY' else 'Sell',
            'amount': round(_contracts(agg['size']), 8),
            'average_filled_price': round(agg['avg_price'], 8),
            'total_fees': round(agg['commission'], 8),
            'status': 'FILLED',
            'reconstructed': True,
        }
        autil.runinsert(
            "INSERT INTO liveevent (scriptid, eventtype, eventdata, time) "
            "VALUES (?,?,?,?)",
            (scriptid, et, json.dumps(d), at))
        local_by_oid[oid] = True
        summary['inserted'] += 1

    # ---- 5. Recompute ProfitAfterFees on every exit event -----------------
    completed = _load_round_trips(scriptid)
    for rt in completed:
        if not rt.get('exit_id') or not rt['entry_dir'] or not rt['entry_price'] \
                or not rt['exit_price'] or not rt['exit_contracts']:
            continue
        sign = 1.0 if rt['entry_dir'] == 'Buy' else -1.0
        gross = sign * (rt['exit_price'] - rt['entry_price']) * \
            rt['exit_contracts'] * contract_size
        net = gross - rt['total_fees']
        try:
            erows = autil.runselect(
                "SELECT eventdata FROM liveevent WHERE id=?", (rt['exit_id'],))
            d = json.loads(erows[0]['eventdata']) if erows and erows[0]['eventdata'] else {}
        except Exception:
            d = {}
        if abs(float(d.get('ProfitAfterFees', 0) or 0) - net) > 0.005:
            d['ProfitAfterFees'] = round(net, 4)
            d['reconciled'] = True
            autil.runupdate("UPDATE liveevent SET eventdata=? WHERE id=?",
                            (json.dumps(d), rt['exit_id']))
            summary['pnl_updated'] += 1

    summary['round_trips'] = len(completed)
    summary['contract_size'] = contract_size
    summary['size_ratio'] = ratio
    return JSONResponse(summary)


@router.get("/live/tick_detail")
async def live_tick_detail(session: str = Depends(require_session),
                           event_id: int = Query(...)):
    import re
    from datetime import datetime
    import calendar as _calendar

    scriptid = autil.getkeyval('live_scriptid')
    if not scriptid:
        return JSONResponse({'events': [], 'simlog': []})

    clicked = autil.runselect("SELECT * FROM liveevent WHERE id=?", (event_id,))
    if not clicked:
        raise HTTPException(status_code=404, detail="Event not found")

    # Find the tick event at or after the clicked id (tick events close the batch)
    tick_row = autil.runselect(
        "SELECT * FROM liveevent WHERE scriptid=? AND eventtype='tick' AND id >= ? ORDER BY id ASC LIMIT 1",
        (int(scriptid), event_id))
    if not tick_row:
        tick_row = autil.runselect(
            "SELECT * FROM liveevent WHERE scriptid=? AND eventtype='tick' AND id <= ? ORDER BY id DESC LIMIT 1",
            (int(scriptid), event_id))
    if not tick_row:
        return JSONResponse({'events': clicked, 'simlog': []})

    tick_event = tick_row[0]
    tick_id = tick_event['id']
    tick_time = tick_event['time']

    # Find the previous tick event to bound the window
    prev_tick = autil.runselect(
        "SELECT * FROM liveevent WHERE scriptid=? AND eventtype='tick' AND id < ? ORDER BY id DESC LIMIT 1",
        (int(scriptid), tick_id))
    start_id = (prev_tick[0]['id'] + 1) if prev_tick else 0
    prev_time = prev_tick[0]['time'] if prev_tick else (tick_time - 3600)

    events = autil.runselect(
        "SELECT * FROM liveevent WHERE scriptid=? AND id >= ? AND id <= ? ORDER BY id ASC",
        (int(scriptid), start_id, tick_id))

    # Filter live_log lines within the tick's time window.
    # Continuation lines (no timestamp, e.g. traceback lines) are included
    # if they follow a line that was already accepted. The log is scoped per
    # scriptid so switching algorithms doesn't surface the previous script's
    # log in history.
    live_log = autil.getkeyval(f'live_log_{scriptid}') or ''
    simlog_lines = []
    in_window = False
    for line in live_log.split('\n'):
        if not line.strip():
            continue
        m = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) UTC', line)
        if m:
            try:
                dt = datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S')
                ts = _calendar.timegm(dt.timetuple())
                in_window = prev_time <= ts <= tick_time + 10
            except Exception:
                in_window = False
        if in_window:
            simlog_lines.append(line)

    return JSONResponse({'events': events, 'simlog': simlog_lines})


@router.get("/key_permissions")
async def key_permissions(session: str = Depends(require_session)):
    """Diagnostic: return what the configured Coinbase API key is allowed to do."""
    from coinbase_http import CoinbaseHTTP
    try:
        data = CoinbaseHTTP().get_key_permissions()
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({'error': str(e)}, status_code=200)


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
    import re
    match = re.search(r'granularity\s*=\s*["\'](\w+)["\']', scripts[0]['script'])
    granularity = match.group(1) if match else 'ONE_HOUR'
    # Prefer Product_ID (new), fall back to legacy pair= for older scripts.
    match_pid = re.search(r'Product_ID\s*=\s*["\']([^"\']+)["\']', scripts[0]['script'])
    if match_pid:
        product_id = match_pid.group(1)
    else:
        match_pair = re.search(r'pair\s*=\s*["\'](\w+)["\']', scripts[0]['script'])
        product_id = (match_pair.group(1).upper() + '-PERP-INTX') if match_pair else 'BTC-PERP-INTX'
    return JSONResponse({'granularity': granularity, 'product_id': product_id})


# ============================== Futures product discovery ==============================

@router.post("/futures/refresh")
async def futures_refresh(session: str = Depends(require_session)):
    """Enumerate FUTURE products visible to the configured key, upsert into the local
    cache. Returns counts so the UI can show 'Loaded N products (M tradeable)'."""
    from coinbase_http import CoinbaseHTTP
    cb = CoinbaseHTTP()
    autil.clear_futures_products()
    total = 0
    tradeable = 0
    cursor = None
    try:
        while True:
            data = cb.list_products(product_type='FUTURE', cursor=cursor)
            products = data.get('products') or []
            for p in products:
                total += 1
                autil.upsert_futures_product(p)
                fd = p.get('future_product_details') or {}
                region = fd.get('region_enabled') or {}
                if not p.get('view_only') and region.get('US'):
                    tradeable += 1
            pagination = data.get('pagination') or {}
            cursor = pagination.get('next_cursor') or data.get('cursor')
            if not cursor or not products:
                break
    except Exception as e:
        return JSONResponse({'error': str(e), 'total': total, 'tradeable': tradeable},
                            status_code=200)
    return JSONResponse({'total': total, 'tradeable': tradeable})


@router.get("/futures/cryptos")
async def futures_cryptos(session: str = Depends(require_session)):
    """Distinct contract_root_units across tradeable products in the cache."""
    return JSONResponse({'cryptos': autil.list_futures_cryptos()})


@router.get("/futures/products")
async def futures_products(session: str = Depends(require_session),
                           root_unit: str = Query(...)):
    """All tradeable products for one root unit (view_only==false, US enabled)."""
    rows = autil.list_futures_products_by_root(root_unit.upper())
    return JSONResponse({'products': rows})


@router.get("/futures/product/{product_id}")
async def futures_product_detail(product_id: str, session: str = Depends(require_session)):
    """Fresh fetch of one product's full JSON; also refresh the cache row."""
    from coinbase_http import CoinbaseHTTP
    try:
        data = CoinbaseHTTP().get_product(product_id)
        if isinstance(data, dict) and data.get('product_id'):
            autil.upsert_futures_product(data)
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({'error': str(e)}, status_code=200)

