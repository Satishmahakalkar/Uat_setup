import datetime
import logging
from algos.basealgo import BaseAlgo
from database.models import Account, Algo, Instrument, Interval, Ltp, Ohlc, Position, Subscription, TradeExit, TradeSide


class PriceBandExitAlgo(BaseAlgo):

    async def init(self):
        self.algo = await Algo.get(name=self.__class__.__name__)

    async def get_todays_price(self, instrument: Instrument) -> float:
        ltp = await Ltp.filter(instrument=instrument).get()
        return float(ltp.price)
    
    async def get_yesterdays_price(self, position: Position) -> float:
        instrument = position.instrument
        today = datetime.date.today()
        ohlc = await Ohlc.filter(instrument=instrument, interval=Interval.EOD, timestamp__lt=today).order_by('-timestamp').first()
        yesterdays_price = ohlc.close
        trade_exit = await TradeExit.filter(position=position).select_related('entry_trade').get()
        if trade_exit.entry_trade.timestamp.date() == today:
            entry_price = float(trade_exit.entry_trade.price)
            if position.side == TradeSide.BUY:
                return max(entry_price, yesterdays_price)
            else:
                return min(entry_price, yesterdays_price)
        return yesterdays_price

    async def run(self):
        account_ids = await Subscription.filter(algo=self.algo, active=True).values_list('account_id', flat=True)
        accounts = await Account.filter(id__in=account_ids)
        for account in accounts:
            subs = await Subscription.filter(account=account, active=True, is_hedge=False).values_list('id', flat=True)
            positions = await Position.filter(
                subscription_id__in=subs, active=True, instrument__future_id__isnull=False
            ).select_related('instrument__future__stock')
            for position in positions:
                stock = position.instrument.future.stock
                try:
                    todays_price = await self.get_todays_price(position.instrument)
                    yesterdays_price = await self.get_yesterdays_price(position)
                except Exception as ex:
                    logging.error(f"Error in getting price {stock}", exc_info=ex)
                    continue
                long = position.buy_price and not position.sell_price
                short = position.sell_price and not position.buy_price
                if (
                    (long and todays_price < (yesterdays_price * (1 - 0.015)))
                    or (short and todays_price > (yesterdays_price * (1 + 0.015)))
                ):
                    logging.info(f"Exiting {stock} for price {todays_price}, yesterdays price {yesterdays_price}")
                    try:
                        await self.exit(position, todays_price)
                    except Exception as ex:
                        logging.error(f"Error in exiting position {stock}", exc_info=ex)
                else:
                    logging.info(f"Not Exiting {stock} for price {todays_price}, yesterdays price {yesterdays_price}")