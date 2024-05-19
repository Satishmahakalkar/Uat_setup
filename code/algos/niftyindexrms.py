from decimal import Decimal
from typing import Optional
from algos.basealgo import BaseAlgoPnlRMS
from database.models import Instrument, Investment, Ltp, Position, Stock, Subscription, SubscriptionData, TradeSide
from tortoise.exceptions import DoesNotExist
from tortoise.functions import Sum
import datetime


class NiftyIndexRMS(BaseAlgoPnlRMS):

    async def init(self, *args, **kwargs) -> None:
        await super().init("strategy7", "Nifty50", **kwargs)
        self.index_stock = await Stock.filter(ticker='NIFTY 50').get()
        self.index_future_instrument = await Instrument.filter(
            future__stock=self.index_stock,
            future__expiry__gt=datetime.date.today()
        ).order_by('future__expiry').select_related('future').first()

    async def entry(self, sub: Subscription, instrument: Instrument, qty: int, side: TradeSide, price: float):
        if instrument == self.index_future_instrument:
            return await super().entry(sub, instrument, qty, side, price)
        
    def get_qty(self, investment: Decimal, invest_per_stock: Decimal, instrument: Instrument, price: float) -> int:
        if instrument != self.index_future_instrument:
            return instrument.future.lot_size
        else:
            return int((investment * 5 // 1250000) * instrument.future.lot_size)
        
    async def run_trades_from_shadow(self, subscription: Subscription, side: Optional[TradeSide] = None):
        if not side:
            return
        ltp = await Ltp.filter(instrument=self.index_future_instrument).get()
        account = await subscription.account
        investment = await Investment.filter(account=account).annotate(
            sum=Sum('amount')
        ).first().values_list('sum', flat=True)
        qty = self.get_qty(investment, investment, self.index_future_instrument, ltp.price)
        await self.entry(subscription, self.index_future_instrument, qty, side, ltp.price)

    async def run(self):
        await super().run()
        instrument = self.index_future_instrument
        subs = await Subscription.filter(active=True, algo=self.algo).select_related('account')
        ltp = await Ltp.filter(instrument=instrument).get()
        price = ltp.price
        for sub in subs:
            try:
                sub_data = await SubscriptionData.filter(subscription=sub).get()
                long_entry_allowed: bool = sub_data.data['long_entry_allowed']
                short_entry_allowed: bool = sub_data.data['short_entry_allowed']
            except (DoesNotExist, KeyError):
                long_entry_allowed = False
                short_entry_allowed = False
            try:
                nifty_gap_exit_sub = await Subscription.filter(account=sub.account, algo=self.nifty_gap_exit_algo).get()
                sub_data = await SubscriptionData.filter(subscription=nifty_gap_exit_sub).get()
                long_nifty_exit: bool = sub_data.data['long_nifty_exit']
                short_nifty_exit: bool = sub_data.data['short_nifty_exit']
            except (DoesNotExist, KeyError):
                long_nifty_exit = False
                short_nifty_exit = False
            position = await Position.filter(subscription=sub, active=True).get_or_none()
            investment = await Investment.filter(account=sub.account).annotate(
                sum=Sum('amount')
            ).first().values_list('sum', flat=True)
            qty = self.get_qty(investment, investment, instrument, price)
            if (
                (position and position.side == TradeSide.BUY)
                and (not long_entry_allowed or short_entry_allowed or long_nifty_exit)
            ) or (
                (position and position.side == TradeSide.SELL)
                and (not short_entry_allowed or long_entry_allowed or short_nifty_exit)
            ):
                await self.exit(position, price)
                position = None
            elif (
                not position
                and long_entry_allowed
                and not short_entry_allowed
                and not long_nifty_exit
            ):
                await self.entry(sub, instrument, qty, TradeSide.BUY, price)
            elif (
                not position
                and short_entry_allowed
                and not long_entry_allowed
                and not short_nifty_exit
            ):
                await self.entry(sub, instrument, qty, TradeSide.SELL, price)
