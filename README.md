# coinbaseperptradingbot
NOT READY

A way to build a trading algorithm for coin base perpetual futures with backtesting

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
There are some settings in config.txt if you want to have TLS or login
## Coinbase Perpetuals
This programs abstracts coinbase's trading into simple enter and exit long/shorts. In your algorithm you can either enter long, enter short, exit long, exit short. And within that you can create a market order, Limit Order (favorible movement) Stop order (unfavorible movement), or Bracket order (limit and stop combined). Each entry ties up your funds until it is filled. so if you have 1000usd and you create an entry long for 1000 you won't be able to create any other orders. But for exits it doesn't not tie up funds at all. Coinbase limits you to 500 total orders. You can only have one market position at a time, which is either positive (long) or negative (short) with coinbase. Two long entries that get filled with result in 1 long position that is a sum of the two. Exiting Long will neutralize your entire long position allowing you to move into a short position.  

Coinbase doesn't use the notion of entry and exits for perpetuals. Under the hoos it works completely different which is why it is abstracted to make writing algorithms more reasonable. When you buy it just enters a long position and a sell you enter a short postion. So it ties up fund to sell just like buy. But if you currently have a long position then a sell will by an exit, and if the sell exceeds your current positon it will exit all of your longs and enter into a short postion in the same transaction. But of course it does change if it ties up your funds, so the trading software needs to keep track of all that to have it execute correctly. There is a special flag that they have for perpetuals called reduce-only = true that will ensure that transaction does not increase or open a new postion. This will ensure that the limit/stop/bracket order doesn't accidentally tie up your funds when attempting to exit the position.  
## Indicators
Your algorithm scripts will have acces to all the TA lib indiators (everything you can imagine). You can also write your own custom indicators. The trading scripts are in python. Your script just needs to have a single indicators function if you want to use it.
```python
def indicators():
  sma_5 = talib.SMA(closes, timeperiod=10)
  mysma = (opens[-1]+opens[-2]+opens[-3])/3.0
  return {'mysma':mysma, 'sma5':sma_5}
```
## Trade Events
The entire program trading simulator operates on trade events. Each tick you can return as many as you want. The backtest will indicate an event that was requested by the user's algorithm, one that actually would have been created if all the exchange requirements were met, and any open order that would have been filled if market conditions were met. You have access to the candle data with candle['close'] or open, high, low, timestamp. You also have access to historicle data with candles[-1]. You also have access to volume and volumes[]. also opens[] closes[] highs[] lows[]. So technically you don't need indicators at all, you could do everything in tick. history size is 100 ticks. Addtional variables are makerfee, takerfee, pendingpositions[], time, usd, btc, eth. 
```python
def tick():
  mysma = calcinds['mysma']
  sma5 = calcinds['sma5']
  diff = (mysma[-1] - sma5[-1])/sma5[-1]
  if(diff is nan):
    return []
  if(diff > 0.01):
    return [TradeOrder(tradetype=TradeType.EnterLong, amount=100, limitprice=90000)]
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
