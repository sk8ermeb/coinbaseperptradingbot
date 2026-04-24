# coinbaseperptradingbot
NOT READY FOR LIVE. Beta for everything else. Ready to build out algorithms and run backtesting with an actual coinbase connection. I am going to say this is permanantly for educational purposes only because I am just building this for myself. 
## Description
A Web based tool to build trading algorihtms for Coinbase US perpetual futures advanced trade API. Has complete back testing capability. Should be a close approximation for live testing but of course bugs can happen and nuaonces around real time market data versus candles will always differ. 
## Algorithm Developement
Algorithms are built in python. See the wiki for complete requirements. There is a default example in the software of my understanding of a simple mean regression algorithn
![Alt text](https://github.com/user-attachments/assets/53f0838f-43a6-43f8-a8e6-2fb6f3f3fee0)
## BackTesting
Simply pick the script and time spand and let it run. The system will analyze for errors and show you all the results. See the wiki for a detailed analysis of how this works
![Alt text](https://github.com/user-attachments/assets/e8411090-68bb-4af7-89f6-062f671ad403)
![Alt text](https://github.com/user-attachments/assets/5f5dec2f-af8c-416e-a59e-8da51ce14cfe)
## Live Trading
Just pick your same script and hit play and let it keep track of the rest.
![Alt text](ttps://github.com/user-attachments/assets/add5b59b-c077-4437-afd0-2f5fcb2e61fd)

# Requirements
Should run on any OS. Must have python3.13 in your path. python3.13 --version or python --version
I am just one guy so I don't want to spend the time to version test accross different
dependencies. But most are pretty generic and should work on older pythons. 
# Install
## Linux/MacOS
./setup.sh
## Windows
open up a git bash window (it comes with git on windows). Navigate to the repo and 
run ./setup.sh

# Use case
Open a browser and go 127.0.0.1:8080
There are some settings in config.txt if you want to have TLS or login. 
## Coinbase Perpetuals
This Program only work with US coinbase perpetual futures. At this point only bitcoin and etherium are available. You definitely need to understand how perpetual futures work to make sense of this. Most importantly how trading on margin works. Coinbase only has buy and sell to move contracts from one direction to another. This programs abstracts that into Buy, Sell, and Exit. Buying enters long, Sell enters short, Exit exits the current contract. This program also has implemented trailing limit and stop orders. These are not available on coinbase so the engine handles it but tracking market movement every tick and re-adjusting limit and stop orders. The simulation should also handle leverages liquidation events the same so you can make smart decisions about how much leverage is appropriate for a certain strategy. 
## Indicators
Indicators are calculated once over the the time frame in back testing. So any code you put in the indicators function will only happen once before the simulation runs on the historical candles. So put as much as you can in this part. Technically you could calculate all your indicators just the same in tick() but then that would be a low slower. 
```python
def indicators():
  sma_5 = talib.SMA(closes, timeperiod=10)
  mysma = (opens[-1]+opens[-2]+opens[-3])/3.0
  return {'mysma':mysma, 'sma5':sma_5}
```
## Trade Events
The entire program trading simulator operates on trade events. Each tick you can return as many as you want. The backtest will indicate an event that was requested by the user's algorithm, one that actually would have been created if all the exchange requirements were met, and any open order that would have been filled if market conditions were met. You have access to the candle data with candle['close'] or open, high, low, timestamp. You also have access to historicle data with candles[-1]. You also have access to volume and volumes[]. also opens[] closes[] highs[] lows[]. So technically you don't need indicators at all, you could do everything in tick. history size is 300 ticks to match coinbase. See the wiki for a complete list of variable available and all the TradeOrder arguments. 
```python
def tick():
  mysma = calcinds['mysma']
  sma5 = calcinds['sma5']
  diff = (mysma[-1] - sma5[-1])/sma5[-1]
  if(diff is nan):
    return []
  if(diff > 0.01):
    return [TradeOrder(tradetype=TradeType.Buy, amount=0, limitprice=90000)]
  return []
```
## Full Script
A full script just has, tick, indicators and the following global variables:
### Required
pair='btc'
granularity='ONE_HOUR'
### Optional Overrides
usd = 1000.0
maxpositions = 1
makerfee = 0.0003
takerfee = 0.0001
# No Warrantee
If you use this code you do so without any warrantee whatsoever. I am not liable for anything that goes wrong
I am just writing this for myself and if other tech people want to see what I have done it is on them.

# Other Licenses
### bootstrap
https://getbootstrap.com/
### Code Mirror
https://codemirror.net/
### Trading View (Light wieght charts)
https://github.com/tradingview/lightweight-charts
