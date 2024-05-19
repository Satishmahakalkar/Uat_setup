import datetime
import importlib
import logging
import aiohttp
import numpy as np
from typing import Dict, List, Literal, Optional, Tuple
from decimal import Decimal
from database.models import *
from strategies import strategy as StrategyModule
from tortoise.transactions import in_transaction
from tortoise.expressions import Subquery
from tortoise.functions import Sum
from tortoise.exceptions import DoesNotExist


class BaseAlgo:

    def __init__(self) -> None:
        self.trades: List[Trade] = []
        self.algo: Algo = None

    async def init(self):
        raise NotImplementedError
    
    async def run(self):
        raise NotImplementedError
    
    @staticmethod
    def charges_calculate(qty: int, price: float, side=TradeSide.BUY):
        price = float(price)
        value = abs(qty) * price
        brokerage = 0.0001 * value
        stt = 0.000125 * value if side == TradeSide.SELL else 0
        exchange = 0.000019 * value
        stamp_duty = 0.00002 * value if side == TradeSide.BUY else 0
        sebi = (value / 10000000) * 10
        gst = 0.18 * (brokerage + sebi + exchange)
        total_charges = brokerage + stt + exchange + stamp_duty + sebi + gst
        return Decimal(total_charges)
    
    async def entry(self, sub: Subscription, instrument: Instrument, qty: int, side: TradeSide, price: float, reversal: bool = False):
        if qty == 0:
            return
        trade = await Trade.create(
                subscription=sub,
                instrument=instrument,
                side=side,
                qty=qty,
                price=price
            )
        position = await Position.create(
            subscription=sub,
            instrument=instrument,
            qty=qty,
            side=side,
            buy_price=None,
            sell_price=None,
            charges=self.charges_calculate(qty, price, side),
            pnl=0.0,
            active=True,
            reversal=reversal
        )
        if side == TradeSide.BUY:
            position.buy_price = price
        elif side == TradeSide.SELL:
            position.sell_price = price
        await position.save()
        await TradeExit.create(
            entry_trade=trade,
            position=position,
            exit_trade=None
        )
        self.trades.append(trade)
        return trade

    async def exit(self, position: Position,  price: float):
        side = TradeSide.SELL if position.side == TradeSide.BUY else TradeSide.BUY
        await position.fetch_related('subscription', 'instrument')
        trade = await Trade.create(
            subscription=position.subscription,
            instrument=position.instrument,
            side=side,
            qty=position.qty,
            price=price
        )
        trade_exit = await TradeExit.filter(position=position).get()
        trade_exit.exit_trade = trade
        await trade_exit.save()
        if position.side == TradeSide.BUY:
            position.sell_price = Decimal(price)
        elif position.side == TradeSide.SELL:
            position.buy_price = Decimal(price)
        position.charges = (
            self.charges_calculate(position.qty, position.buy_price, TradeSide.BUY)
            + self.charges_calculate(position.qty, position.sell_price, TradeSide.SELL)
        )
        position.pnl = (position.sell_price - position.buy_price) * position.qty
        position.active = False
        await position.save()
        self.trades.append(trade)
        return trade
    
    async def rollover(self):
        today = datetime.date.today()
        positions = await Position.filter(
            subscription__active=True,
            subscription__algo=self.algo,
            active=True,
            instrument__future__expiry__lte=today
        ).select_related('instrument__future__stock')
        async with in_transaction():
            for position in positions:
                ltp = await Ltp.filter(instrument=position.instrument).get()
                instrument = await Instrument.filter(future__stock=position.instrument.future.stock, future__expiry__gt=today).order_by('future__expiry').first()
                await self.exit(position, ltp.price)
                ltp = await Ltp.filter(instrument=instrument).get()
                await self.entry(
                    sub=position.subscription,
                    instrument=instrument,
                    qty=position.qty,
                    side=position.side,
                    price=ltp.price
                )


class BaseAlgoStrat(BaseAlgo):

    async def init(self, strategy_name: str, stock_group_name: str, exit_only=False, net_new=False):
        self.strategy_obj = await Strategy.get(name=strategy_name)
        self.algo = await Algo.get(name=self.__class__.__name__)
        self.stock_group = await StockGroup.get(name=stock_group_name)
        self.exit_only = exit_only
        self.net_new = net_new
        self.strategy: StrategyModule = importlib.import_module(f"strategies.{self.strategy_obj.name}")

    async def get_data_for_stock(self, stock: Stock) -> List[float]:
        return await Ohlc.filter(instrument__stock=stock, interval=Interval.EOD, timestamp__lt=datetime.date.today()).order_by('-timestamp').limit(365).values_list('close', flat=True)

    async def get_price_for_stock(self, stock: Stock) -> float:
        ltp = await Ltp.filter(instrument__stock=stock).get()
        return ltp.price
    
    async def get_yesterdays_price_for_stock(self, stock: Stock) -> float:
        ohlc_1, ohlc_2 = await Ohlc.filter(instrument__stock=stock, interval=Interval.EOD).order_by('-timestamp').limit(2)
        if ohlc_1.timestamp.date() < datetime.date.today():
            return ohlc_1.close
        else:
            return ohlc_2.close

    async def get_price_for_future(self, future: Future) -> float:
        ltp = await Ltp.filter(instrument__future=future).get()
        return ltp.price
    
    def is_buy_allowed(self, *args) -> bool:
        if self.exit_only:
            return False
        else:
            return True

    async def exit_positions(self, positions: List[Position]):
        for position in positions:
            await position.fetch_related('instrument__future')
            price = await self.get_price_for_future(position.instrument.future)
            await self.exit(position, price)

    async def get_investment_per_stock(self, investment):
        return (investment * 5 / 40) * Decimal(1.10)

    async def buy_sell(self, subscriptions: List[Subscription], stock: Stock, side: TradeSide):
        today = datetime.date.today()
        try:
            future = await Future.filter(stock=stock, expiry__gt=today).order_by('expiry').first()
            instrument = await Instrument.filter(future=future).select_related('future').get()
        except Exception as ex:
            logging.error(f"Error in getting future for {stock}", exc_info=ex)
            return
        price = await self.get_price_for_future(instrument.future)
        for sub in subscriptions:
            await sub.fetch_related('account')
            investment_sum = await Investment.filter(account=sub.account).annotate(sum=Sum('amount')).first().values('sum')
            investment = investment_sum['sum']
            invest_per_stock = await self.get_investment_per_stock(investment)
            qty = int(invest_per_stock // Decimal(instrument.future.lot_size * price)) * instrument.future.lot_size
            await self.entry(sub, instrument, qty, side, price)

    async def run(self):
        subscriptions = Subscription.filter(algo=self.algo, active=True)
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
            portfolios = Position.filter(
                subscription__id__in=Subquery(subscriptions.values('id')),
                instrument__future__stock=stock,
                active=True
            )
            try:
                if side == 'BUY':
                    to_exit = await portfolios.filter(side=TradeSide.SELL)
                    if self.is_buy_allowed():
                        subs = portfolios.filter(side=TradeSide.BUY).values('subscription__id')
                        to_buy = await subscriptions.exclude(id__in=Subquery(subs))
                        await self.buy_sell(to_buy, stock, TradeSide.BUY)
                    else:
                        logging.error(f"Buy not allowed for {stock}")
                elif side == 'SELL':
                    to_exit = await portfolios.filter(side=TradeSide.BUY)
                    if self.is_buy_allowed():
                        subs = portfolios.filter(side=TradeSide.SELL).values('subscription__id')
                        to_sell = await subscriptions.exclude(id__in=Subquery(subs))
                        await self.buy_sell(to_sell, stock, TradeSide.SELL)
                    else:
                        logging.error(f"Sell not allowed for {stock}")
                else:
                    to_exit = await portfolios.filter(side__in=(TradeSide.BUY, TradeSide.SELL))
                await self.exit_positions(to_exit)
            except Exception as ex:
                logging.error(f"Error in trade for {stock}", exc_info=ex)


class BaseAlgoExit(BaseAlgoStrat):

    async def exit_all(self):
        positions = await Position.filter(subscription__algo=self.algo, subscription__active=True, active=True)
        await self.exit_positions(positions)

    async def run(self):
        await self.exit_all()


class BaseAlgoPnlRMS(BaseAlgoStrat):

    async def init(self, *args, mode: Literal["REGULAR", "RECTIFICATION"] = "REGULAR", shadow_only=False, **kwargs) -> None:
        self.mode = mode
        self.shadow_only = shadow_only
        self.nifty_gap_exit_algo = await Algo.get(name="NiftyGapExit")
        await super().init(*args, **kwargs)

    def get_qty(self, investment: Decimal, invest_per_stock: Decimal, instrument: Instrument, price: float) -> int:
        if investment == Decimal(5000000):
            return instrument.future.lot_size
        else:
            return int(invest_per_stock // Decimal(instrument.future.lot_size * price)) * instrument.future.lot_size

    async def net_new_entry(self, subscriptions: List[Subscription], side_map: Dict[Stock, Literal['BUY', 'SELL', 'HOLD']]):
        for sub in subscriptions:
            positions = await Position.filter(subscription=sub, active=True)
            await self.exit_positions(positions)
            investment_sum = await Investment.filter(account=sub.account).annotate(sum=Sum('amount')).first().values('sum')
            investment = investment_sum['sum']
            invest_per_stock = await self.get_investment_per_stock(investment)
            store_positions = []
            net_new_blocks = {}
            for stock, side in side_map.items():
                if not side == 'HOLD':
                    future = await Future.filter(stock=stock, expiry__gt=datetime.date.today()).order_by('expiry').first()
                    instrument = await Instrument.filter(future=future).select_related('future__stock').get()
                    ltp = await Ltp.get(instrument=instrument)
                    price = ltp.price
                    qty = self.get_qty(investment, invest_per_stock, instrument, price)
                    store_positions.append({
                        'inst_id': instrument.id,
                        'price': float(price),
                        'side': side.lower(),
                        'qty': int(qty),
                        'entry_time': datetime.datetime.now().isoformat()
                    })
                    if self.net_new:
                        net_new_blocks[stock.ticker] = side
            await SubscriptionData.create(
                subscription=sub,
                data={
                    'positions': store_positions,
                    'net_new_blocks': net_new_blocks
                }
            )

    async def exit_positions(self, positions: List[Position]):
        if not self.shadow_only:
            return await super().exit_positions(positions)

    async def entry(self, sub: Subscription, instrument: Instrument, qty: int, side: TradeSide, price: float):
        if not self.shadow_only:
            return await super().entry(sub, instrument, qty, side, price)

    async def run_mtm_update(self):
        subs_q = Subscription.filter(active=True, algo=self.algo)
        sub_datas = await SubscriptionData.filter(id__in=Subquery(subs_q))
        for sub_data in sub_datas:
            stored_positions = sub_data.data.get('positions', [])
            for values in stored_positions:
                if not values['exit_time']:
                    ltp = await Ltp.filter(id=values['inst_id']).get()
                    if TradeSide(values['side']) == TradeSide.BUY:
                        values['mtm'] = values['qty'] * (ltp.price * values['old_price'])
                    else:
                        values['mtm'] = values['qty'] * (values['old_price'] - ltp.price)
            sub_data.data['positions'] = stored_positions
            await sub_data.save()

    async def run_trades_from_shadow(self, subscription: Subscription, side: Optional[TradeSide] = None):
        sub_data = await SubscriptionData.filter(subscription=subscription).get()
        stored_positions = sub_data.data.get('positions', [])
        active_insts = await Position.filter(active=True, subscription_id=sub_data.subscription_id).values_list('instrument_id', flat=True)
        today = datetime.date.today()
        for values in stored_positions:
            if not values.get('exit_time') and values['inst_id'] not in active_insts:
                if side and TradeSide(values['side']) != side:
                    continue
                instrument = await Instrument.get(id=values['inst_id'])
                ltp = await Ltp.filter(instrument=instrument).get()
                await self.entry(subscription, instrument, values['qty'], TradeSide(values['side']), ltp.price)
            elif values.get('exit_time') and values['inst_id'] in active_insts:
                position = await Position.filter(subscription=subscription, active=True, instrument_id=values['inst_id']).get()
                exit_date = datetime.datetime.fromisoformat(values['exit_time']).date()
                if position.side == TradeSide(values['side']) and exit_date == today:
                    ltp = await Ltp.filter(instrument=instrument).get()
                    await self.exit(position, ltp.price)

    async def run(self):
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
            side_map[stock] = side
        subscriptions = await Subscription.filter(algo=self.algo, active=True).select_related('account')
        net_new_subs = []
        to_exit = set()
        today = datetime.date.today()
        for sub in subscriptions:
            try:
                sub_data = await SubscriptionData.get(subscription=sub)
            except DoesNotExist:
                net_new_subs.append(sub)
                continue
            stored_positions: List[dict] = sub_data.data.get('positions', [])
            net_new_blocks: dict = sub_data.data.get('net_new_blocks', {})
            trade_allowed: bool = sub_data.data.get('trade_allowed', True)
            try:
                nifty_gap_sub = await Subscription.filter(account=sub.account, algo=self.nifty_gap_exit_algo, active=True).get()
                nifty_gap_sub_data = await SubscriptionData.filter(subscription=nifty_gap_sub).get()
                long_nifty_exit = nifty_gap_sub_data.data['long_nifty_exit']
                short_nifty_exit = nifty_gap_sub_data.data['short_nifty_exit']
            except (DoesNotExist, KeyError):
                long_nifty_exit = False
                short_nifty_exit = False
            if self.mode == 'REGULAR':
                long_mtm, short_mtm = 0, 0
                stored_positions_changed = []
                for values in stored_positions:
                    instrument = await Instrument.get(id=values['inst_id']).select_related('future__stock')
                    ltp = await Ltp.get(instrument=instrument)
                    if 'exit_time' in values:
                        if datetime.datetime.fromisoformat(values['exit_time']).date() < today:
                            continue
                        else:
                            new_price = values['exit_price']
                    else:
                        if TradeSide(values['side']).name != side_map.get(instrument.future.stock):
                            values['exit_price'] = ltp.price
                            values['exit_time'] = datetime.datetime.now().isoformat()
                            net_new_blocks.pop(instrument.future.stock.ticker, None)
                        new_price = ltp.price
                    if datetime.datetime.fromisoformat(values['entry_time']).date() < today:
                        ohlc = await Ohlc.filter(instrument=instrument, interval=Interval.EOD, timestamp__lt=today).order_by('-timestamp').first()
                        old_price = ohlc.close
                    else:
                        old_price = values['price']
                    if TradeSide(values['side']) == TradeSide.BUY:
                        mtm = (new_price - old_price) * values['qty']
                        long_mtm += mtm
                    elif TradeSide(values['side']) == TradeSide.SELL:
                        mtm = (old_price - new_price) * values['qty']
                        short_mtm += mtm
                    values['mtm'] = mtm
                    values['price'] = new_price
                    values['old_price'] = old_price
                    stored_positions_changed.append(values)
                stored_positions = stored_positions_changed
                long_entry_allowed = long_mtm > 0
                short_entry_allowed = short_mtm > 0
                sub_data.data['long_entry_allowed'] = long_entry_allowed
                sub_data.data['short_entry_allowed'] = short_entry_allowed
            elif self.mode =='RECTIFICATION':
                long_entry_allowed: bool = sub_data.data.get('long_entry_allowed', False)
                short_entry_allowed: bool = sub_data.data.get('short_entry_allowed', False)
                for values in stored_positions:
                    instrument = await Instrument.get(id=values['inst_id']).select_related('future__stock')
                    ltp = await Ltp.get(instrument=instrument)
                    if 'exit_time' not in values and TradeSide(values['side']).name != side_map.get(instrument.future.stock):
                        values['exit_price'] = ltp.price
                        values['exit_time'] = datetime.datetime.now().isoformat()
                        net_new_blocks.pop(instrument.future.stock.ticker, None)
                    ## fluff for saving in sheet
                    new_price = ltp.price
                    if datetime.datetime.fromisoformat(values['entry_time']).date() < today:
                        ohlc = await Ohlc.filter(instrument=instrument, interval=Interval.EOD, timestamp__lt=today).order_by('-timestamp').first()
                        old_price = ohlc.close
                    else:
                        old_price = values['price']
                    if TradeSide(values['side']) == TradeSide.BUY:
                        mtm = (new_price - old_price) * values['qty']
                    elif TradeSide(values['side']) == TradeSide.SELL:
                        mtm = (old_price - new_price) * values['qty']
                    values['price'] = values.get('exit_price', ltp.price)
                    values['old_price'] = old_price
                    values['mtm'] = mtm
            stock_sides: List[Tuple[Stock, TradeSide]] = []
            for values in stored_positions:
                instrument = await Instrument.filter(id=values['inst_id']).get().select_related('future__stock')
                if 'exit_time' not in values:
                    stock_sides.append((instrument.future.stock, TradeSide(values['side'])))
            for stock, side in side_map.items():
                position = await Position.filter(
                    subscription=sub,
                    instrument__future__stock=stock,
                    active=True
                ).get_or_none()
                if side != 'HOLD':
                    future = await Future.filter(stock=stock, expiry__gt=datetime.date.today()).order_by('expiry').first()
                    instrument = await Instrument.filter(future=future).select_related('future__stock').get()
                    ltp = await Ltp.get(instrument=instrument)
                    price = ltp.price
                    trade_side = TradeSide.BUY if side == 'BUY' else TradeSide.SELL
                    investment = await Investment.filter(account=sub.account).annotate(sum=Sum('amount')).first().values_list('sum', flat=True)
                    invest_per_stock = await self.get_investment_per_stock(investment)
                    qty = self.get_qty(investment, invest_per_stock, instrument, price)
                    if (stock, trade_side) not in stock_sides and not self.exit_only:
                        stored_positions.append({
                            'inst_id': instrument.id,
                            'price': float(price),
                            'side': trade_side.value,
                            'qty': int(qty),
                            'entry_time': datetime.datetime.now().isoformat()
                        })
                        net_new_blocks.pop(instrument.future.stock.ticker, None)
                    if position and position.side != trade_side:
                        to_exit.add(position)
                    elif (
                        not self.exit_only
                        and trade_allowed
                        and (stock.ticker not in net_new_blocks)
                        and (not position or (position and position.side != trade_side))
                        and ((long_entry_allowed and trade_side == TradeSide.BUY) or (short_entry_allowed and trade_side == TradeSide.SELL))
                        and ((not long_nifty_exit and trade_side == TradeSide.BUY) or (not short_nifty_exit and trade_side == TradeSide.SELL))
                    ):
                        await self.entry(sub, instrument, qty, trade_side, price)
                elif position:
                    to_exit.add(position)
            if (not long_entry_allowed) or long_nifty_exit:
                positions = await Position.filter(subscription=sub, active=True, side=TradeSide.BUY)
                for pos in positions:
                    to_exit.add(pos)
            if (not short_entry_allowed) or short_nifty_exit:
                positions = await Position.filter(subscription=sub, active=True, side=TradeSide.SELL)
                for pos in positions:
                    to_exit.add(pos)
            sub_data.data['positions'] = stored_positions
            sub_data.data['net_new_blocks'] = net_new_blocks
            await sub_data.save()
        await self.exit_positions(to_exit)
        await self.net_new_entry(net_new_subs, side_map)

    async def rollover(self):
        await super().rollover()
        today = datetime.date.today()
        sub_datas = await SubscriptionData.filter(subscription__active=True, subscription__algo=self.algo)
        for sub_data in sub_datas:
            stored_positions = sub_data.data.get('positions', [])
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


class BaseRMSCapAlloc(BaseAlgoPnlRMS):

    async def init(self, *args, **kwargs) -> None:
        await super().init(*args, **kwargs)
        self.margin_map = {}

    async def _init_margin_map(self):
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.kite.trade/margins/futures") as res:
                data = await res.json()
        today = datetime.date.today()
        self.margin_map = {rec['tradingsymbol'].partition(today.strftime("%y"))[0] : float(rec['margin']) for rec in data}

    async def get_nifty_investment(self, account: Account):
        if not self.margin_map:
            await self._init_margin_map()
        positions = await Position.filter(subscription__account=account, active=True).select_related('instrument__future__stock')
        mu = 0
        for pos in positions:
            if pos.instrument.future:
                ltp = await Ltp.filter(instrument=pos.instrument).get()
                mu += pos.qty * ltp.price * self.margin_map[pos.instrument.future.stock.ticker] / 100
        investment = await Investment.filter(account=account).annotate(sum_investment=Sum('amount')).first().values_list('sum_investment', flat=True)
        investment = float(investment)
        mu_ratio = (mu / investment) * 100
        nfu_ratio = max(90 - mu_ratio, 0)
        nfu = nfu_ratio * investment
        return nfu
    
    async def run(self):
        await super().run()
        subs = await Subscription.filter(algo=self.algo, active=True).select_related('account')
        today = datetime.date.today()
        instrument = await Instrument.filter(
            future__stock__ticker="NIFTY 50",
            future__expiry__gt=today
        ).order_by('future__expiry').select_related('future').first()
        ltp = await Ltp.filter(instrument=instrument).get()
        price = ltp.price
        for sub in subs:
            sub_data = await SubscriptionData.filter(subscription=sub).get()
            long_entry_allowed = sub_data.data.get('long_entry_allowed', False)
            short_entry_allowed = sub_data.data.get('short_entry_allowed', False)
            try:
                nifty_gap_sub = await Subscription.filter(account=sub.account, algo=self.nifty_gap_exit_algo, active=True).get()
                nifty_gap_sub_data = await SubscriptionData.filter(subscription=nifty_gap_sub).get()
                long_nifty_exit = nifty_gap_sub_data.data['long_nifty_exit']
                short_nifty_exit = nifty_gap_sub_data.data['short_nifty_exit']
            except (DoesNotExist, KeyError):
                long_nifty_exit = False
                short_nifty_exit = False
            nifty_investment = await self.get_nifty_investment(sub.account)
            qty = int(nifty_investment // (ltp.price * instrument.future.lot_size))
            position = await Position.filter(instrument=instrument, subscription=sub, active=True).get_or_none()
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
