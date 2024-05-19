import datetime
import logging
from algos.niftygapexit import NiftyGapExit
from database.models import Interval, Ltp, Ohlc, Position, Subscription, TradeSide


class NiftyPriceBandExit(NiftyGapExit):

    async def init(self, *args, **kwargs):
        await super().init(*args, **kwargs)
        self.nifty_movement_allowed = 0.4

    async def nifty_price_increase_percent(self, is_long=True) -> float:
        ohlc1, ohlc2 = await Ohlc.filter(instrument__stock__ticker=self.index_ticker, interval=Interval.EOD).order_by('-timestamp').limit(2)
        ltp = await Ltp.filter(instrument__stock__ticker=self.index_ticker).get()
        if is_long:
            compare_price = max(ohlc1.open, ohlc2.close)
        else:
            compare_price = min(ohlc1.open, ohlc2.close)
        logging.info(f"Nifty Price comparison: is_long {is_long}, compare_price {compare_price}, price {ltp.price}")
        return ((ltp.price - compare_price) * 100 / compare_price)

    async def run(self):
        nifty_price_increase_long = await self.nifty_price_increase_percent(is_long=True)
        nifty_price_increase_short = await self.nifty_price_increase_percent(is_long=False)
        short_exit = nifty_price_increase_short > self.nifty_movement_allowed
        long_exit = nifty_price_increase_long < -(self.nifty_movement_allowed)
        account_ids = await Subscription.filter(algo=self.algo, active=True).values_list('account_id', flat=True)
        sub_ids = await Subscription.filter(account_id__in=account_ids, active=True, is_hedge=False).values_list('id', flat=True)
        positions_q = Position.filter(
            subscription_id__in=sub_ids, active=True, instrument__future_id__isnull=False
        ).select_related('instrument__future__stock')
        if short_exit:
            positions = await positions_q.filter(side=TradeSide.SELL)
        elif long_exit:
            positions = await positions_q.filter(side=TradeSide.BUY)
        else:
            positions = []
        for position in positions:
            ltp = await Ltp.filter(instrument=position.instrument).get()
            await self.exit(position, ltp.price)