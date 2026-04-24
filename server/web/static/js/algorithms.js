
const defscript = 
`#Put Your Code Here
# Mean Reversion Strategy

# Logic: SMA_SHORT vs SMA_LONG divergence signals a likely mean-reversion.
#   - Short SMA well BELOW long SMA and slope turning up → limit buy (bet on bounce up)
#   - Short SMA well ABOVE long SMA and slope turning down → limit sell/short (bet on drop)
# Thresholds are scaled by a volatility index.
# Exits use a trailing stop that gives the position room to breathe.
#

pair = "btc"
granularity = "ONE_HOUR"
leverage = 3          # Lower than max — mean reversion can be underwater a while
maxpositions = 1      #This strategy only allows for 1 position at a time.

# Strategy parameters (matching original defaults)
MA_SHORT = 4          #4 hour sma
MA_LONG  = 96         #4 day sma
DEVIATION = 0.012      # This is how far the small SMA must drift from the long SMA to signar an entry
LIMIT_OFFSET = 0.005   # 0.5% better-than- the current price. ensures we get maker fee and the best price
CANCEL_DRIFT = 0.03   # cancel signal if market drifts 3% from our limit
STOP_PCT  = 0.10     # How much of a loss do we take before we consider it a run to limit losses
TRAIL_PCT = 0.015      # trailing stop distance how much valatility relative to our SMA's to
                      #maximize profits and to actually take gains when it goes back 1.5%
PROFIT = 0.02        # At what point are we willing to start take profit

def indicators():
    ma_s = talib.SMA(closes, timeperiod=MA_SHORT)
    ma_l = talib.SMA(closes, timeperiod=MA_LONG)

    atr_abs = talib.ATR(highs, lows, closes, timeperiod=MA_LONG)

    # Avoid divide-by-zero on early NaN-padded closes
    #this is tweaked so it goes from about 1 to 2.5
    safe_closes = numpy.where(closes == 0, 1, closes)
    atr_pct = atr_abs / safe_closes          # convert to fraction of price
    atr_adj = (atr_pct - 0.0) * 1000      # shift/scale to match original units
    vol_idx = numpy.log10(atr_adj) + 0.72

    return {
        'ma_short': (ma_s,),
        'ma_long':  (ma_l,),
        'vol_idx':  (vol_idx,),
    }

def tick():
    orders = []

    # --- Read indicators ---
    ma_s_arr = calcinds.get('ma_short')
    ma_l_arr = calcinds.get('ma_long')
    vol_arr  = calcinds.get('vol_idx')

    #Return if we don't yet have enough historical data to have computed
    #indicators yet.
    if ma_s_arr is None or ma_l_arr is None or vol_arr is None:
        print("indicators are none!!")
        return orders

    ma_s = ma_s_arr[-1]
    ma_l = ma_l_arr[-1]
    vol  = vol_arr[-1]
    #Return if the indicator arrays are not long enough for our algorithm
    #in this case we just need them to have 1 entry
    if numpy.isnan(ma_s) or numpy.isnan(ma_l) or numpy.isnan(vol):
        return orders

    #This is to cancel pending limit positions if it has drifted to far in the wrong direction
    for order in list(pendingpositions):
          lp = order['limitprice']
          sp = order['stopprice']
          if order['tradetype'] == 'Buy' and order['limitprice'] > 0:
              drift = (close - order['limitprice']) / close
              if drift > CANCEL_DRIFT:
                  cancel_order(order['id'])
          if order['tradetype'] == 'Sell' and order['limitprice'] > 0:
              drift = (close - order['limitprice']) / close
              if drift < -CANCEL_DRIFT:
                  cancel_order(order['id'])

    #this is a fine tuning adjustment to scale the importance of the volatility
    #index multiplier
    entrvolmultiplier = 2
    dvol = vol*entrvolmultiplier - entrvolmultiplier +1

    costbasepercent = (ma_s - costbasis)/costbasis
    percentdrift = (ma_s - ma_l)/ma_l
    #Entry Logic. For this strategy is if there are no held contracts. I.E. realposition = 0
    if realposition == 0:
        if(percentdrift < -DEVIATION*dvol):
            return [TradeOrder(tradetype=TradeType.Buy, limitprice=close*(1-LIMIT_OFFSET))]
        elif(percentdrift > DEVIATION*dvol):
            return [TradeOrder(tradetype=TradeType.Sell, limitprice=close*(1+LIMIT_OFFSET))]

    #Exit logic. For this strategy is if we are holding either short or long contracts and there are no pending limit or stop orders
    if realposition > 0 and not pendingpositions:
        return [TradeOrder(
            TradeType.Exit,
            stopprice  = costbasis * (1-STOP_PCT),
            limitprice = costbasis * (1+PROFIT*vol),
            limittrailpercent = TRAIL_PCT*vol
        )]
    if realposition < 0 and not pendingpositions:
        return [TradeOrder(
            TradeType.Exit,
            stopprice         = costbasis * (1+STOP_PCT),
            limitprice        = costbasis * (1-PROFIT*vol),
            limittrailpercent = TRAIL_PCT*vol
        )]

    return orders
`;
async function handleScriptSelect(select) {
  const selectedId = select.value;           // this is the script.id
  const selectedName = select.options[select.selectedIndex].text;
  if(selectedId > -1){
    localStorage.setItem('selectedScriptId', selectedId);
    document.getElementById('delbtn').classList.remove('d-none');
    const response = await fetch('/api/fetchscript?scriptid=' + selectedId, {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
    });

    if (response.ok) {
      const data = await response.json();
      const script = data['script']
      const name = data['name']
      document.getElementById('scriptheadname').textContent = name;
      window.editor.dispatch({
        changes: { from: 0, to: window.editor.state.doc.length, insert: script }
      });
    } else {
	  showMessage("Failed to load script");
    }
  }
  else{
      document.getElementById('delbtn').classList.add('d-none');
      document.getElementById('scriptheadname').textContent = "New Script";
      window.editor.dispatch({
        changes: { from: 0, to: window.editor.state.doc.length, insert: defscript }
      });
  }
}

function confirmDelete(){
  const select = document.getElementById('myDropdown');
  const selectedId = select.value;
  const selectedText = select.options[select.selectedIndex].text;
  if(selectedId == -1){
    showMessageModal("Can't delete unsaved script");
  }
  else{
    showConfirmModal("Are you sure you want to delete "+selectedText+"? This cannot be undone.", async ()=>{
      const response = await fetch('/api/deletescript/' + selectedId, {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
      });

      if (response.ok) {
        const index = select.selectedIndex;
        select.remove(index);          // removes the selected option
        select.selectedIndex = 0;      // selects the new first item
        document.getElementById('scriptheadname').textContent = "New Script";
        window.editor.dispatch({
          changes: { from: 0, to: window.editor.state.doc.length, insert: "#Write your python code here" }
        });
        document.getElementById('delbtn').classList.add('d-none');
      }
      else{
      showMessageModal("Failed to delete script");
      }
      
    });
  }

}

function confirmSave(){
  const select = document.getElementById('myDropdown');
  const selectedId = select.value;
  const selectedText = select.options[select.selectedIndex].text;
  if(selectedId != -1){
    document.getElementById('scriptname').value = selectedText;
  }
  else{
    document.getElementById('scriptname').value = "";
  }
  const modal = new bootstrap.Modal(document.getElementById('saveNewScriptModal'));
  modal.show();

}

async function handleScriptSave(){
  const select = document.getElementById('myDropdown');
  const selectedId = select.value;
  const selectedText = select.options[select.selectedIndex].text;
  const name =  document.getElementById('scriptname').value;
  const currentCode = window.editor.state.doc.toString();
  const response = await fetch('/api/savescript', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ scriptid:selectedId, scriptname: name , script: currentCode}),
  });
  bootstrap.Modal.getInstance(document.getElementById('saveNewScriptModal')).hide();
  if (response.ok) {
    showMessage("Script "+name+" saved");
    if(selectedId == -1){
      const data = await response.json();
      const newscriptid = data['scriptid']
      const select = document.getElementById('myDropdown');
      const newOption = document.createElement('option');
      newOption.value = newscriptid;
      newOption.textContent = name;
      document.getElementById('scriptheadname').textContent = name;
      newOption.selected = true;  // makes it selected
      select.insertBefore(newOption, select.options[1]);
    }
    else{
      select.options[select.selectedIndex].text = name;
      document.getElementById('scriptheadname').textContent = name;
    }
  } else {
    showMessage("Failed to save script");
  }
}
document.addEventListener('DOMContentLoaded', () => {
  const lastId = localStorage.getItem('selectedScriptId');
  const select = document.getElementById('myDropdown');
  if (lastId) {
    for (const opt of select.options) {
      if (opt.value === lastId) {
        opt.selected = true;
        handleScriptSelect(select);
        return;
      }
    }
  }
  window.editor.dispatch({
    changes: { from: 0, to: window.editor.state.doc.length, insert: defscript }
  });
});
