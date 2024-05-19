import datetime
from decimal import Decimal
from typing import Optional
from algos.shadowanalysis import ShadowAnalysis
from database.models import Account, Instrument, Investment, Ltp, Position, Stock, Subscription, SubscriptionData, TradeSide
from tortoise.functions import Sum
from tortoise.exceptions import MultipleObjectsReturned


class NiftyIndexShadowAnalysis(ShadowAnalysis):

    async def init(self, **kwargs):
        await super().init("strategy7", "Nifty50", **kwargs)
        self.index_stock = await Stock.filter(ticker='NIFTY 50').get()
        self.index_future_instrument = await Instrument.filter(
            future__stock=self.index_stock,
            future__expiry__gt=datetime.date.today()
        ).order_by('future__expiry').select_related('future').first()

    async def get_qty(self, instrument: Instrument, account: Account) -> int:
        investment = await Investment.filter(
            account=account
        ).annotate(sum=Sum('amount')).first().values_list('sum', flat=True)
        await instrument.fetch_related('future')
        if instrument != self.index_future_instrument:
            investment = Decimal(15000000)
            invest_per_stock = await self.get_investment_per_stock(investment)
            price = await self.get_current_price(instrument)
            return int(invest_per_stock // Decimal(instrument.future.lot_size * price)) * instrument.future.lot_size
        else:
            return int((investment * 10 // 1250000) * instrument.future.lot_size)
        
    async def get_qty_partial(self, instrument: Instrument, account: Account):
        investment = await Investment.filter(
            account=account
        ).annotate(sum=Sum('amount')).first().values_list('sum', flat=True)
        if instrument == self.index_future_instrument:
            return int(((investment / 3) * 10 // 1250000) * instrument.future.lot_size)
        else:
            return await self.get_qty(instrument, account)
        
    async def exit_reversed(self, sub_data: SubscriptionData, side: TradeSide):
        await sub_data.fetch_related('subscription')
        opposite_side = TradeSide.SELL if side == TradeSide.BUY else TradeSide.BUY
        pos = await Position.filter(
            subscription=sub_data.subscription,
            instrument=self.index_future_instrument,
            side=opposite_side,
            active=True
        ).first()
        if pos:
            ltp = await Ltp.filter(instrument_id=self.index_future_instrument.id).get()
            await self.exit(pos, ltp.price)

    async def exit_from_shadow(self, *args, **kwargs):
        pass

    async def enter_reverse_from_shadow(self, sub_data: SubscriptionData, side: TradeSide):
        if side == TradeSide.BUY:
            point = -1
        else:
            point = 1
        sub_data.data['index_points'] = sub_data.data.get('index_points', 0) + point
        await self.entry_for_index(sub_data)

    async def enter_from_shadow(self, sub_data: SubscriptionData, side: TradeSide = None, partial: bool = False):
        if side == TradeSide.BUY:
            point = 1
        else:
            point = -1
        sub_data.data['index_points'] = sub_data.data.get('index_points', 0) + point
        await self.entry_for_index(sub_data)

    async def entry_for_index(self, sub_data: SubscriptionData):
        index_points = sub_data.data.get('index_points', 0)
        if index_points > 0:
            side_to_enter = TradeSide.BUY
        elif index_points < 0:
            side_to_enter = TradeSide.SELL
        else:
            side_to_enter = None
        subscription: Subscription = await sub_data.subscription
        try:
            pos = await Position.filter(
                subscription=subscription,
                instrument=self.index_future_instrument,
                active=True
            ).get_or_none()
        except MultipleObjectsReturned:
            if side_to_enter == TradeSide.BUY:
                await self.exit_all(sub_data, TradeSide.SELL)
            else:
                await self.exit_all(sub_data, TradeSide.BUY)
            return
        account = await subscription.account
        price = await self.get_current_price(self.index_future_instrument)
        if pos and pos.side != side_to_enter:
            await self.exit(pos, price)
        if side_to_enter and (not pos or pos.side != side_to_enter):
            qty = await self.get_qty(self.index_future_instrument, account)
            await self.entry(subscription, self.index_future_instrument, qty, side_to_enter, price)
