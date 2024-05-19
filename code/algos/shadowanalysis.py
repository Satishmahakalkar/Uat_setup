import datetime
from decimal import Decimal
import importlib
import logging
from typing import Dict, List, Literal, Optional, Tuple, TypedDict, Set

import numpy as np
import pytz
from algos.basealgo import BaseAlgo
from database.models import *
from strategies import strategy as StrategyModule
from tortoise.expressions import Subquery
from tortoise.functions import Sum


class ShadowPosition(TypedDict):
    inst_id: int
    price: float
    side: str
    qty: int
    entry_time: str
    entry_price: float
    exit_time: Optional[str]
    exit_price: Optional[float]
    old_price: Optional[float]
    mtm: Optional[float]
    days_high_mtm: Optional[float]


class ShadowAnalysis(BaseAlgo):

    async def init(self, strategy_name: str, stock_group_name: str, shadow_mode: str = "NOOP", trade_mode: str = "NOOP"):
        self.strategy_obj = await Strategy.get(name=strategy_name)
        self.algo = await Algo.get(name=self.__class__.__name__)
        self.stock_group = await StockGroup.get(name=stock_group_name)
        self.strategy: StrategyModule = importlib.import_module(f"strategies.{self.strategy_obj.name}")
        self.shadow_mode: Literal["SHADOW", "SHADOW_MTM", "SHADOW_EXIT", "NOOP", "VALUES_RESET"] = shadow_mode
        self.trade_mode: Literal["ENTRY", "EXIT", "NOOP", "SHADOWCHECK", "SHADOWCHECKREVERSE", "SHADOWCHECKEXITONLY", "SHADOWEXIT"] = trade_mode
        self._stock_calls = {}

    @staticmethod
    def max_value_at_risk(investment: Decimal) -> float:
        return (200000 / 15000000) * float(investment)

    @staticmethod
    def get_mtm_threshold(investment) -> float:
        return (750000 / 15000000) * float(investment)

    @staticmethod
    async def get_old_price(instrument: Instrument) -> float:
        ohlc = await Ohlc.filter(
            instrument=instrument, interval=Interval.EOD,
            timestamp__lt=datetime.date.today()
        ).order_by('-timestamp').first()
        return ohlc.close

    @staticmethod
    async def get_current_price(instrument: Instrument) -> float:
        ltp = await Ltp.filter(instrument=instrument).get()
        return ltp.price

    async def get_investment_per_stock(self, investment):
        if self.stock_group.name == "Nifty50":
            return (investment * 5 / 40) * Decimal(1.10)
        else:
            total_stocks = await StockGroupMap.filter(stock_group=self.stock_group).count()
            return (investment * 5 / total_stocks) * Decimal(1.10)

    async def get_qty(self, instrument: Instrument, account: Account) -> int:
        investment = await Investment.filter(
            account=account
        ).annotate(sum=Sum('amount')).first().values_list('sum', flat=True)
        await instrument.fetch_related('future')
        if investment == Decimal(5000000):
            return instrument.future.lot_size
        else:
            invest_per_stock = await self.get_investment_per_stock(investment)
            price = await self.get_current_price(instrument)
            return int(invest_per_stock // Decimal(instrument.future.lot_size * price)) * instrument.future.lot_size
    
    async def get_qty_partial(self, instrument: Instrument, account: Account):
        investment = await Investment.filter(
            account=account
        ).annotate(sum=Sum('amount')).first().values_list('sum', flat=True)
        await instrument.fetch_related('future')
        invest_per_stock = await self.get_investment_per_stock(investment / 3)
        price = await self.get_current_price(instrument)
        qty = int(invest_per_stock // Decimal(instrument.future.lot_size * price))
        return max(qty, 1) * instrument.future.lot_size

    async def get_data_for_stock(self, stock: Stock) -> List[float]:
        return await Ohlc.filter(instrument__stock=stock, interval=Interval.EOD, timestamp__lt=datetime.date.today()).order_by('-timestamp').limit(365).values_list('close', flat=True)

    async def get_price_for_stock(self, stock: Stock) -> float:
        ltp = await Ltp.filter(instrument__stock=stock).get()
        return ltp.price

    def should_add_stoploss(self, mtm_tracking_arr: List[float]):
        if len(mtm_tracking_arr) < 3:
            return False
        start_mtm = max(mtm_tracking_arr[0], mtm_tracking_arr[1])
        now_mtm = mtm_tracking_arr[-1]
        return (
            start_mtm > 200000
            or start_mtm < -200000
        ) and (
            -600000 < now_mtm < 600000
        )
    
    def get_stoploss(self, start_mtm, mtm):
        if start_mtm > 200000:
            stoploss = mtm - 200000
        elif start_mtm < -200000:
            stoploss = mtm + 200000
        else:
            stoploss = None
        return stoploss
    
    def sl_hit(self, stoploss: Optional[float], mtm: float):
        if stoploss and mtm:
            return (
                stoploss > 0 and mtm < stoploss
            ) or (
                stoploss < 0 and mtm > stoploss
            )
        return False    

    async def get_shadow_mtms(self, sub_data: SubscriptionData):
        long_mtm, short_mtm = 0, 0
        long_count, short_count = 0, 0
        shadow_positions: List[ShadowPosition] = sub_data.data.get('positions', [])
        for values in shadow_positions:
            if TradeSide(values['side']) == TradeSide.BUY:
                long_mtm += values['mtm']
                if not 'exit_time' in values:
                    long_count += 1
            elif TradeSide(values['side']) == TradeSide.SELL:
                short_mtm += values['mtm']
                if not 'exit_time' in values:
                    short_count += 1
        long_days_high_mtm = max([*sub_data.data.get('long_mtm_tracking', []), long_mtm])
        short_days_high_mtm = max([*sub_data.data.get('short_mtm_tracking', []), short_mtm])
        try:
            mtm_920_long = sub_data.data['long_mtm_tracking'][0]
            mtm_920_short = sub_data.data['short_mtm_tracking'][0]
            mtm_930_long = sub_data.data['long_mtm_tracking'][1]
            mtm_930_short = sub_data.data['short_mtm_tracking'][1]
            long_start_mtm = max(mtm_930_long, mtm_920_long)
            short_start_mtm = max(mtm_930_short, mtm_920_short)
            long_reset_mtm = long_mtm - long_start_mtm
            short_reset_mtm = short_mtm - short_start_mtm
        except (KeyError, IndexError):
            long_reset_mtm, short_reset_mtm = 0.0, 0.0
            long_start_mtm, short_start_mtm = 0, 0
        return (
            long_mtm,
            short_mtm,
            long_days_high_mtm,
            short_days_high_mtm,
            long_start_mtm,
            short_start_mtm,
            long_count,
            short_count,
            long_reset_mtm,
            short_reset_mtm
        )

    def should_enter(self, investment: Decimal, mtm: float, positions_count: int, days_high_mtm: float, entry_count: int, reset_mtm: float):
        investment = 15000000       # keeping fixed amount
        max_value_at_risk = self.max_value_at_risk(investment)
        return (
            mtm > (investment * 10 * 0.01 * 0.15 * 0.01 * positions_count)
            and (mtm / days_high_mtm) - 1 > -(50 * 0.01)
            and mtm < max_value_at_risk
            and entry_count <= 2
            and reset_mtm > 0
        )
    
    def should_enter_with_sl(self, investment: Decimal, mtm: float, positions_count: int, days_high_mtm: float, entry_count: int, reset_mtm: float, start_mtm: float):
        investment = 15000000       # keeping fixed amount
        return (
            mtm > (investment * 10 * 0.01 * 0.15 * 0.01 * positions_count)
            and (mtm / days_high_mtm) - 1 > -(50 * 0.01)
            and entry_count <= 2
            and reset_mtm > 0
            and abs(mtm) < 400000
            and abs(start_mtm) > 200000
        )

    def should_exit(self, investment: Decimal, mtm: float, positions_count: int, days_high_mtm: float, exit_count: int, is_on_going: bool, reset_mtm: float):
        investment = 15000000
        return (
            (
                not is_on_going
                and (
                    mtm < 0.0
                    or reset_mtm < 0.0
                    or mtm > self.get_mtm_threshold(investment)
                )
            ) or (
                is_on_going 
                and ((
                    positions_count <= 20
                    and mtm < 0.0
                ) or (
                    positions_count > 20
                    and ((mtm / days_high_mtm) - 1 < -(50 * 0.01))
                ))
            )
        )

    def should_reverse(self, investment: Decimal, mtm: float, reset_mtm: float, positions_count, opposite_count):
        investment = float(investment)
        max_value_at_risk = 180000
        point_15_percent = investment * 10 * 0.01 * 0.15 * 0.01 * positions_count
        return ((
            positions_count >= opposite_count
        )
        and ((
            mtm > 0
            and reset_mtm < 0
            and abs(reset_mtm) > point_15_percent
            and reset_mtm > -(max_value_at_risk)
            and mtm < max_value_at_risk * 2
        ) or (
            mtm < 0
            and abs(mtm) < point_15_percent
            and abs(reset_mtm) > point_15_percent
            and reset_mtm > -(max_value_at_risk)
        ) or (
            mtm < 0
            and abs(mtm) > point_15_percent
            and mtm > -(max_value_at_risk)
        )))

    def should_exit_reverse(self, mtm, reset_mtm):
        return (
            mtm > 0 and reset_mtm > 0
        )

    async def generate_stock_calls(self) -> Dict[Stock, TradeSide]:
        if self._stock_calls:
            return self._stock_calls
        side_map = {}
        stock_ids = StockGroupMap.filter(stock_group=self.stock_group).values('stock__id')
        stocks = await Stock.filter(id__in=Subquery(stock_ids))
        for stock in stocks:
            logging.info(f"Running algo for {stock}")
            price_array = await self.get_data_for_stock(stock)
            price_array = np.array(price_array)
            price = await self.get_price_for_stock(stock)
            try:
                side = self.strategy.process(price_array, price)
            except Exception as ex:
                logging.error(f"Could not process strategy for {stock}", exc_info=ex)
                continue
            logging.info(f"{stock} is {side}")
            try:
                side_map[stock] = TradeSide(side.lower())
            except ValueError:
                pass
        self._stock_calls = side_map
        return self._stock_calls

    async def update_shadow_position_mtm(self, shadow_position: ShadowPosition) -> ShadowPosition:
        today = datetime.date.today()
        instrument = await Instrument.filter(id=shadow_position['inst_id']).get()
        if datetime.datetime.fromisoformat(shadow_position['entry_time']).date() < today:
            old_price = await self.get_old_price(instrument)
        else:
            old_price = shadow_position['price']
        shadow_position['old_price'] = old_price
        price = await self.get_current_price(instrument)
        price = shadow_position.get('exit_price', price)
        if TradeSide(shadow_position['side']) == TradeSide.BUY:
            shadow_position['mtm'] = (price - old_price) * shadow_position['qty']
        else:
            shadow_position['mtm'] = (old_price - price) * shadow_position['qty']
        return shadow_position

    async def save_shadow_portfolio(self, sub_data: SubscriptionData, stock_calls: Dict[Stock, TradeSide], exit_only=False):
        shadow_positions: List[ShadowPosition] = sub_data.data.get('positions', [])
        now = datetime.datetime.now()
        today = now.date()
        new_shadow_positions = []
        stocks_in_shadow = set()
        banned_stocks = sub_data.data.get('banned_stocks', [])
        for shadow_position in shadow_positions:
            exit_time = shadow_position.get('exit_time')
            instrument = await Instrument.filter(
                id=shadow_position['inst_id']
            ).select_related('future__stock').get()
            if exit_time and datetime.datetime.fromisoformat(exit_time).date() < today:
                continue
            elif (
                TradeSide(shadow_position['side']) != stock_calls.get(instrument.future.stock)
                or instrument.future.stock.ticker in banned_stocks
            ):
                shadow_position['exit_time'] = now.isoformat()
                price = await self.get_current_price(instrument)
                shadow_position['exit_price'] = price
                await self.update_shadow_position_mtm(shadow_position)
                new_shadow_positions.append(shadow_position)
            else:
                await self.update_shadow_position_mtm(shadow_position)
                new_shadow_positions.append(shadow_position)
                stocks_in_shadow.add(instrument.future.stock)
        if not exit_only:
            for stock, side in stock_calls.items():
                if stock not in stocks_in_shadow and stock.ticker not in banned_stocks:
                    instrument = await Instrument.filter(
                        future__stock=stock,
                        future__expiry__gt=today
                    ).order_by('future__expiry').first()
                    price = await self.get_current_price(instrument)
                    await sub_data.fetch_related('subscription__account')
                    qty = await self.get_qty(instrument, sub_data.subscription.account)
                    new_shadow_positions.append({
                        'inst_id': instrument.id,
                        'price': float(price),
                        'side': side.value,
                        'qty': int(qty),
                        'entry_time': now.isoformat(),
                        'old_price': float(price),
                        'mtm': 0.0
                    })
        sub_data.data['positions'] = new_shadow_positions
        await sub_data.save()

    async def update_shadow_mtm(self, sub_data: SubscriptionData):
        shadow_positions: List[ShadowPosition] = sub_data.data.get('positions', [])
        for shadow_position in shadow_positions:
            await self.update_shadow_position_mtm(shadow_position)
        sub_data.data['positions'] = shadow_positions
        await sub_data.save()

    async def enter_from_shadow(self, sub_data: SubscriptionData,  side: Optional[TradeSide] = None, partial: Optional[bool] = False):
        if not sub_data.data.get('trade_allowed', True):
            return
        shadow_positions: List[ShadowPosition] = sub_data.data.get('positions', [])
        await sub_data.fetch_related('subscription__account')
        for shadow_position in shadow_positions:
            if side and side != TradeSide(shadow_position['side']):
                continue
            position = await Position.filter(
                subscription=sub_data.subscription,
                active=True,
                instrument_id=shadow_position['inst_id']
            ).get_or_none()
            if not shadow_position.get('exit_time') and not position:
                instrument = await Instrument.filter(id=shadow_position['inst_id']).get()
                ltp = await Ltp.filter(instrument=instrument).get()
                partial_qty = await self.get_qty_partial(instrument, sub_data.subscription.account)
                qty = shadow_position['qty'] if not partial else partial_qty
                await self.entry(
                    sub_data.subscription, instrument, 
                    qty,
                    TradeSide(shadow_position['side']),
                    ltp.price,
                    reversal=False
                )

    async def enter_reverse_from_shadow(self, sub_data: SubscriptionData, side: TradeSide):
        if not sub_data.data.get('trade_allowed', True):
            return
        shadow_positions: List[ShadowPosition] = sub_data.data.get('positions', [])
        await sub_data.fetch_related('subscription')
        opposite_side = TradeSide.SELL if side == TradeSide.BUY else TradeSide.BUY
        for shadow_position in shadow_positions:
            if side != TradeSide(shadow_position['side']) or shadow_position.get('exit_time'):
                continue
            instrument = await Instrument.filter(id=shadow_position['inst_id']).get()
            position = await Position.filter(
                subscription=sub_data.subscription,
                active=True,
                instrument=instrument
            ).get_or_none()
            ltp = await Ltp.filter(instrument=instrument).get()
            if position and position.side == side:
                await self.exit(position, ltp.price)
            elif position and position.side == opposite_side:
                continue
            await self.entry(
                sub_data.subscription, instrument, 
                shadow_position['qty'],
                opposite_side,
                ltp.price,
                reversal=True
            )

    async def exit_from_shadow(self, sub_data: SubscriptionData, side: Optional[TradeSide] = None):
        shadow_positions: List[ShadowPosition] = sub_data.data.get('positions', [])
        await sub_data.fetch_related('subscription')
        shadow_set: Set[Tuple[int, TradeSide]] = set()
        position_set: Set[Tuple[int, TradeSide]] = set()
        for shadow_position in shadow_positions:
            if side and TradeSide(shadow_position['side']) != side:
                continue
            if not shadow_position.get('exit_time'):
                shadow_set.add((shadow_position['inst_id'], TradeSide(shadow_position['side'])))
        positions = await Position.filter(subscription=sub_data.subscription, active=True).select_related('instrument')
        for position in positions:
            if side and position.side != side:
                continue
            position_set.add((position.instrument.id, position.side))
        to_exit = position_set - shadow_set
        for inst_id, side in to_exit:
            position = await Position.filter(
                subscription=sub_data.subscription,
                active=True,
                instrument_id=inst_id,
                side=side
            ).get_or_none()
            if position:
                ltp = await Ltp.filter(instrument_id=inst_id).get()
                await self.exit(position, ltp.price)

    async def exit_reversed(self, sub_data: SubscriptionData, side: TradeSide):
        shadow_positions: List[ShadowPosition] = sub_data.data.get('positions', [])
        await sub_data.fetch_related('subscription')
        opposite_side = TradeSide.SELL if side == TradeSide.BUY else TradeSide.BUY
        for shadow_position in shadow_positions:
            if not shadow_position.get('exit_time') and TradeSide(shadow_position['side']) == side:
                position = await Position.filter(
                    active=True,
                    subscription=sub_data.subscription,
                    instrument_id=shadow_position['inst_id'],
                    side=opposite_side,
                    reversal=True
                ).select_related('instrument').get_or_none()
                if position:
                    ltp = await Ltp.filter(instrument=position.instrument).get()
                    await self.exit(position, ltp.price)

    async def exit_all(self, sub_data: SubscriptionData, side: Optional[TradeSide] = None):
        shadow_positions: List[ShadowPosition] = sub_data.data.get('positions', [])
        await sub_data.fetch_related('subscription')
        for shadow_position in shadow_positions:
            if not shadow_position.get('exit_time') and TradeSide(shadow_position['side']) == side:
                position = await Position.filter(
                    active=True,
                    subscription=sub_data.subscription,
                    instrument_id=shadow_position['inst_id'],
                    side=side
                ).select_related('instrument').get_or_none()
                if position:
                    ltp = await Ltp.filter(instrument=position.instrument).get()
                    await self.exit(position, ltp.price)

    async def run(self):
        subscriptions = await Subscription.filter(algo=self.algo, active=True).select_related('account')
        for sub in subscriptions:
            sub_data, _ = await SubscriptionData.get_or_create(subscription=sub, defaults=dict(data={}))
            shadow_long_status: Literal["ENTERED", "EXITED", "REVERSED", "ENTEREDSL"] = sub_data.data.get('shadow_long_status', 'EXITED')
            shadow_short_status: Literal["ENTERED", "EXITED", "REVERSED", "ENTEREDSL"] = sub_data.data.get('shadow_short_status', 'EXITED')
            long_entry_count, long_exit_count = sub_data.data.get('long_entry_count', 0), sub_data.data.get('long_exit_count', 0)
            short_entry_count, short_exit_count = sub_data.data.get('short_entry_count', 0), sub_data.data.get('short_exit_count', 0)
            long_kill_switch = sub_data.data.get('long_kill_switch', False)
            short_kill_switch = sub_data.data.get('short_kill_switch', False)
            long_on_going = sub_data.data.get('long_on_going', False)
            short_on_going = sub_data.data.get('short_on_going', False)
            long_sl = sub_data.data.get('long_sl')
            short_sl = sub_data.data.get('short_sl')
            trade_counter = sub_data.data.get('trade_counter', 0)
            if self.shadow_mode == "SHADOW":
                # 9:20 and 3:15
                stock_calls = await self.generate_stock_calls()
                await self.save_shadow_portfolio(sub_data, stock_calls)
            elif self.shadow_mode == "SHADOW_MTM":
                # every 15 mins from 9:30
                await self.update_shadow_mtm(sub_data)
            elif self.shadow_mode == "SHADOW_EXIT":
                stock_calls = await self.generate_stock_calls()
                await self.save_shadow_portfolio(sub_data, stock_calls, exit_only=True)
            (
                long_mtm,
                short_mtm,
                long_days_high_mtm,
                short_days_high_mtm,
                long_start_mtm,
                short_start_mtm,
                long_count,
                short_count,
                long_reset_mtm,
                short_reset_mtm
            ) = await self.get_shadow_mtms(sub_data)
            if not self.shadow_mode == "NOOP":
                sub_data.data.setdefault('long_mtm_tracking', []).append(long_mtm)
                sub_data.data.setdefault('short_mtm_tracking', []).append(short_mtm)
            if self.shadow_mode == "VALUES_RESET":
                sub_data.data.pop('long_mtm_tracking', None)
                sub_data.data.pop('short_mtm_tracking', None)
                sub_data.data.pop('banned_stocks', None)
                sub_data.data.pop('long_stoploss', None)
                sub_data.data.pop('long_stoploss_active', None)
                sub_data.data.pop('short_stoploss', None)
                sub_data.data.pop('short_stoploss_active', None)
                long_entry_count, long_exit_count = 0, 0
                short_entry_count, short_exit_count = 0, 0
                long_kill_switch = False
                short_kill_switch = False
                if shadow_long_status == "ENTERED":
                    long_on_going = True
                else:
                    long_on_going = False
                if shadow_short_status == "ENTERED":
                    short_on_going = True
                else:
                    short_on_going = False
            investment = await Investment.filter(
                account=sub.account
            ).annotate(sum=Sum('amount')).first().values_list('sum', flat=True)
            if self.trade_mode == "EXIT":
                # 3:15
                if shadow_long_status == "REVERSED":
                    await self.exit_reversed(sub_data, TradeSide.BUY)
                    shadow_long_status = "EXITED"
                elif shadow_long_status == "ENTEREDSL":
                    await self.exit_all(sub_data, TradeSide.BUY)
                    shadow_long_status = "EXITED"
                elif (
                    shadow_long_status == "ENTERED" 
                    and self.should_exit(investment, long_mtm, long_count, long_days_high_mtm, long_exit_count, long_on_going, long_reset_mtm)
                ):
                    await self.exit_all(sub_data, TradeSide.BUY)
                    shadow_long_status = "EXITED"
                if shadow_short_status == "REVERSED":
                    await self.exit_reversed(sub_data, TradeSide.SELL)
                    shadow_short_status = "EXITED"
                elif shadow_short_status == "ENTEREDSL":
                    await self.exit_all(sub_data, TradeSide.SELL)
                    shadow_short_status = "EXITED"
                elif (
                    shadow_short_status == "ENTERED"
                    and self.should_exit(investment, short_mtm, short_count, short_days_high_mtm, short_exit_count, short_on_going, short_reset_mtm)
                ):
                    await self.exit_all(sub_data, TradeSide.SELL)
                    shadow_short_status = "EXITED"
                if sub_data.data.get('long_stoploss_active', False):
                    await self.exit_all(sub_data, TradeSide.BUY)
                elif sub_data.data.get('short_stoploss_active', False):
                    await self.exit_all(sub_data, TradeSide.SELL)
                await self.exit_from_shadow(sub_data)
            if self.trade_mode == "NOOP":
                logging.info("Trade Mode NOOP. Not doing anything.")
            if self.trade_mode == "SHADOWEXIT":
                await self.exit_from_shadow(sub_data)
            if self.trade_mode == "ENTRY":
                # 9:45
                logging.info("Trade Mode ENTRY.")
                # should_exit checks to be added
                if (
                    shadow_long_status == "ENTERED" 
                    and self.should_exit(investment, long_mtm, long_count, long_days_high_mtm, long_exit_count, long_on_going, long_reset_mtm)
                ):
                    await self.exit_all(sub_data, TradeSide.BUY)
                    shadow_long_status = "EXITED"
                elif shadow_long_status == "ENTERED":
                    await self.exit_from_shadow(sub_data, TradeSide.BUY)
                    await self.enter_from_shadow(sub_data, TradeSide.BUY)
                elif (
                    shadow_long_status == "EXITED"
                    and self.should_enter(investment, long_mtm, long_count, long_days_high_mtm, long_entry_count, long_reset_mtm)
                ):
                    await self.enter_from_shadow(sub_data, TradeSide.BUY)
                    shadow_long_status = "ENTERED"
                elif (
                    shadow_long_status == "EXITED"
                    and self.should_enter_with_sl(investment, long_mtm, long_count, long_days_high_mtm, long_entry_count, long_reset_mtm, long_start_mtm)
                ):
                    await self.enter_from_shadow(sub_data, TradeSide.BUY)
                    shadow_long_status = "ENTEREDSL"
                    long_sl = self.get_stoploss(long_start_mtm, long_mtm)
                if (
                    shadow_short_status == "ENTERED"
                    and self.should_exit(investment, short_mtm, short_count, short_days_high_mtm, short_exit_count, short_on_going, short_reset_mtm)
                ):
                    await self.exit_all(sub_data, TradeSide.SELL)
                elif shadow_short_status == "ENTERED":
                    await self.exit_from_shadow(sub_data, TradeSide.SELL)
                    await self.enter_from_shadow(sub_data, TradeSide.SELL)
                elif (
                    shadow_short_status == "EXITED"
                    and self.should_enter(investment, short_mtm, short_count, short_days_high_mtm, short_entry_count, short_reset_mtm)
                ):
                    await self.enter_from_shadow(sub_data, TradeSide.SELL)
                    shadow_short_status = "ENTERED" 
                elif (
                    shadow_short_status == "EXITED"
                    and self.should_enter_with_sl(investment, short_mtm, short_count, short_days_high_mtm, short_entry_count, short_reset_mtm, short_start_mtm)
                ):
                    await self.enter_from_shadow(sub_data, TradeSide.SELL)
                    shadow_short_status = "ENTEREDSL"
                    short_sl = self.get_stoploss(short_start_mtm, short_mtm)
            if self.trade_mode == "SHADOWCHECK":
                # every 15 mins from 10:00
                if (
                    shadow_long_status == "ENTERED" 
                    and self.should_exit(investment, long_mtm, long_count, long_days_high_mtm, long_exit_count, long_on_going, long_reset_mtm)
                ):
                    await self.exit_all(sub_data, TradeSide.BUY)
                    shadow_long_status = "EXITED"
                    long_exit_count += 1
                    long_on_going = False
                elif (
                    shadow_long_status == "ENTEREDSL" 
                    and (
                        self.should_exit(investment, long_mtm, long_count, long_days_high_mtm, long_exit_count, long_on_going, long_reset_mtm)
                        or self.sl_hit(long_sl, long_mtm)
                    )
                ):
                    await self.exit_all(sub_data, TradeSide.BUY)
                    shadow_long_status = "EXITED"
                    long_exit_count += 1
                    long_on_going = False
                elif (
                    shadow_long_status == "EXITED"
                    and not long_kill_switch
                    and self.should_enter(investment, long_mtm, long_count, long_days_high_mtm, long_entry_count, long_reset_mtm)
                ):
                    await self.enter_from_shadow(sub_data, TradeSide.BUY)
                    shadow_long_status = "ENTERED"
                    long_entry_count += 1
                elif (
                    shadow_long_status == "EXITED"
                    and not long_kill_switch
                    and self.should_enter_with_sl(investment, long_mtm, long_count, long_days_high_mtm, long_entry_count, long_reset_mtm, long_start_mtm)
                ):
                    await self.enter_from_shadow(sub_data, TradeSide.BUY)
                    shadow_long_status = "ENTEREDSL"
                    long_sl = self.get_stoploss(long_start_mtm, long_mtm)
                    long_entry_count += 1
                if (
                    shadow_short_status == "ENTERED"
                    and self.should_exit(investment, short_mtm, short_count, short_days_high_mtm, short_exit_count, short_on_going, short_reset_mtm)
                ):
                    await self.exit_all(sub_data, TradeSide.SELL)
                    shadow_short_status = "EXITED"
                    short_exit_count += 1
                    short_on_going = False
                elif (
                    shadow_short_status == "ENTEREDSL"
                    and (
                        self.should_exit(investment, short_mtm, short_count, short_days_high_mtm, short_exit_count, short_on_going, short_reset_mtm)
                        or self.sl_hit(short_sl, short_mtm)
                    )
                ):
                    await self.exit_all(sub_data, TradeSide.SELL)
                    shadow_short_status = "EXITED"
                    short_exit_count += 1
                    short_on_going = False
                elif (
                    shadow_short_status == "EXITED"
                    and not short_kill_switch
                    and self.should_enter(investment, short_mtm, short_count, short_days_high_mtm, short_entry_count, short_reset_mtm)
                ):
                    await self.enter_from_shadow(sub_data, TradeSide.SELL)
                    shadow_short_status = "ENTERED"
                    short_entry_count += 1
                elif (
                    shadow_short_status == "EXITED"
                    and not short_kill_switch
                    and self.should_enter_with_sl(investment, short_mtm, short_count, short_days_high_mtm, short_entry_count, short_reset_mtm, short_start_mtm)
                ):
                    await self.enter_from_shadow(sub_data, TradeSide.SELL)
                    shadow_short_status = "ENTEREDSL"
                    short_sl = self.get_stoploss(short_start_mtm, short_mtm)
                    short_entry_count += 1
            if self.trade_mode == "SHADOWCHECKEXITONLY":
                if (
                    shadow_long_status == "ENTERED" 
                    and self.should_exit(investment, long_mtm, long_count, long_days_high_mtm, long_exit_count, long_on_going, long_reset_mtm)
                ):
                    await self.exit_all(sub_data, TradeSide.BUY)
                    shadow_long_status = "EXITED"
                    long_on_going = False
                elif (
                    shadow_long_status == "ENTEREDSL" 
                    and (
                        self.should_exit(investment, long_mtm, long_count, long_days_high_mtm, long_exit_count, long_on_going, long_reset_mtm)
                        or self.sl_hit(long_sl, long_mtm)
                    )
                ):
                    await self.exit_all(sub_data, TradeSide.BUY)
                    shadow_long_status = "EXITED"
                    long_exit_count += 1
                    long_on_going = False
                if (
                    shadow_short_status == "ENTERED"
                    and self.should_exit(investment, short_mtm, short_count, short_days_high_mtm, short_exit_count, short_on_going, short_reset_mtm)
                ):
                    await self.exit_all(sub_data, TradeSide.SELL)
                    shadow_short_status = "EXITED"
                    short_on_going = False
                elif (
                    shadow_short_status == "ENTEREDSL"
                    and (
                        self.should_exit(investment, short_mtm, short_count, short_days_high_mtm, short_exit_count, short_on_going, short_reset_mtm)
                        or self.sl_hit(short_sl, short_mtm)
                    )
                ):
                    await self.exit_all(sub_data, TradeSide.SELL)
                    shadow_short_status = "EXITED"
                    short_exit_count += 1
                    short_on_going = False
            if self.trade_mode == "SHADOWCHECKREVERSE":
                if (
                    shadow_long_status != "REVERSED"
                    and self.should_reverse(investment, long_mtm, long_reset_mtm, long_count, short_count)
                ):
                    long_sl = self.get_stoploss(min(long_mtm, long_reset_mtm), long_mtm)
                    await self.exit_all(sub_data, TradeSide.BUY)
                    await self.enter_reverse_from_shadow(sub_data, TradeSide.BUY)
                    shadow_long_status = "REVERSED"
                elif (
                    shadow_long_status == "REVERSED"
                    and (
                        self.should_exit_reverse(long_mtm, long_reset_mtm)
                        or self.sl_hit(long_sl, long_mtm)
                    )
                ):
                    await self.exit_reversed(sub_data, TradeSide.BUY)
                    shadow_long_status = "EXITED"
                if (
                    shadow_short_status != "REVERSED"
                    and self.should_reverse(investment, short_mtm, short_reset_mtm, short_count, long_count)
                ):
                    short_sl = self.get_stoploss(min(short_mtm, short_reset_mtm), short_mtm)
                    await self.exit_all(sub_data, TradeSide.SELL)
                    await self.enter_reverse_from_shadow(sub_data, TradeSide.SELL)
                    shadow_short_status = "REVERSED"
                elif (
                    shadow_short_status == "REVERSED"
                    and (
                        self.should_exit_reverse(short_mtm, short_reset_mtm)
                        or self.sl_hit(short_sl, short_mtm)
                    )
                ):
                    await self.exit_reversed(sub_data, TradeSide.SELL)
                    shadow_short_status = "EXITED"
            sub_data.data['shadow_short_status'] = shadow_short_status
            sub_data.data['shadow_long_status'] = shadow_long_status
            sub_data.data['long_entry_count'] = long_entry_count
            sub_data.data['long_exit_count'] = long_exit_count
            sub_data.data['short_entry_count'] = short_entry_count
            sub_data.data['short_entry_count'] = short_entry_count
            sub_data.data['long_kill_switch'] = long_kill_switch
            sub_data.data['short_kill_switch'] = short_kill_switch
            sub_data.data['long_on_going'] = long_on_going
            sub_data.data['short_on_going'] = short_on_going
            sub_data.data['long_sl'] = long_sl
            sub_data.data['short_sl'] = short_sl
            await sub_data.save()

    async def rollover(self):
        await super().rollover()
        today = datetime.date.today()
        sub_datas = await SubscriptionData.filter(subscription__active=True, subscription__algo=self.algo)
        for sub_data in sub_datas:
            stored_positions: List[ShadowPosition] = sub_data.data.get('positions', [])
            stored_positions_change = []
            for values in stored_positions:
                instrument = await Instrument.filter(id=values['inst_id']).select_related('future__stock').get()
                if instrument.future.expiry <= today:
                    next_instrument = await Instrument.filter(future__stock=instrument.future.stock, future__expiry__gt=today).order_by('future__expiry').first()
                    values['inst_id'] = next_instrument.id
                    if values.get('old_price'):
                        ohlc = await Ohlc.filter(instrument=next_instrument, interval=Interval.EOD, timestamp__lt=today).order_by('-timestamp').first()
                        values['old_price'] = ohlc.close
                stored_positions_change.append(values)
            sub_data.data['positions'] = stored_positions_change
            await sub_data.save()