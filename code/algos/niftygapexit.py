import datetime
from algos.basealgo import BaseAlgo
from database.models import Algo, Interval, Ltp, Ohlc, Position, Subscription, SubscriptionData, TradeSide
from tortoise.exceptions import DoesNotExist


class NiftyGapExit(BaseAlgo):

    async def init(self):
        self.algo = await Algo.get(name=self.__class__.__name__)
        self.nifty_movement_allowed = 0.25
        self.index_ticker = "NIFTY 50"

    async def nifty_price_increase_percent(self) -> float:
        ohlc1, ohlc2 = await Ohlc.filter(instrument__stock__ticker=self.index_ticker, interval=Interval.EOD).order_by('-timestamp').limit(2)
        if ohlc1.timestamp.date() == datetime.date.today():
            ohlc = ohlc2
        else:
            ohlc = ohlc1
        ltp = await Ltp.filter(instrument__stock__ticker=self.index_ticker).get()
        return ((ltp.price - ohlc.close) * 100 / ohlc.close)
    
    async def run(self):
        nifty_price_increase = await self.nifty_price_increase_percent()
        short_exit = nifty_price_increase > self.nifty_movement_allowed
        long_exit = nifty_price_increase < -(self.nifty_movement_allowed)
        subs = await Subscription.filter(active=True, algo=self.algo)
        for sub in subs:
            to_save_data = {
                'long_nifty_exit': long_exit,
                'short_nifty_exit': short_exit
            }
            try:
                sub_data = await SubscriptionData.get(subscription=sub)
                sub_data.data = to_save_data
                await sub_data.save()
            except DoesNotExist:
                await SubscriptionData.create(
                    subscription=sub,
                    data=to_save_data
                )