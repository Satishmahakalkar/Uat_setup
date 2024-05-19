

import asyncio
import datetime
import itertools
from typing import Callable, Dict, List, Literal, Tuple, TypedDict
from accounts.mail import TradeSplitMailer
from algos.fnobancheck import FnOBanCheck
from algos.resultshedgealgo import ResultsHedgeAlgo
from algos.shadowanalysis import ShadowAnalysis, ShadowPosition
from database.models import Account, Instrument, Investment, Position, Subscription, SubscriptionData, Trade, TradeSide
from tortoise.exceptions import DoesNotExist
from tortoise.functions import Sum


Status = Literal['ENTERED', 'EXITED']

class PositionMetaData(TypedDict):
    splitted: bool
    mtm_tracking: List[float]
    stop_loss: float
    investment: float
    entry_count: int
    exit_count: int
    opposite_count: int
    is_on_going: bool
    normal_status: Status
    reversal_status: Status
    sl_window_status: Status

class PositionMap(TypedDict):
    positions: List[ShadowPosition]
    meta_data: PositionMetaData
    side: Literal['buy', 'sell']
    trade_baskets: dict


class ShadowSplit(ShadowAnalysis):

    async def init(self, *args, **kwargs):
        timer_action = kwargs.pop('timer_action')
        await super().init(*args, **kwargs)
        self.action: Literal['9_20', '9_30', '9_45', '10_to_2_15', '2_30_to_3', '3_15', '3_20'] = timer_action

    async def process(
            self, status: Status,
            should_enter_check: Callable, should_exit_check: Callable, 
            sub: Subscription, position_map: PositionMap, reversal: bool = False
        ) -> Tuple[Status, bool]:
        new_status = status
        if reversal:
            side = TradeSide.BUY if TradeSide(position_map['side']) == TradeSide.SELL else TradeSide.SELL
        else:
            side = TradeSide(position_map['side'])
        if status == 'EXITED' and should_enter_check():
            for shadow_position in position_map['positions']:
                exists = await Position.filter(active=True, subscription=sub, instrument_id=shadow_position['inst_id']).exists()
                if not exists:
                    instrument = await Instrument.filter(id=shadow_position['inst_id']).get()
                    price = await self.get_current_price(instrument)
                    self.entry(sub, instrument, shadow_position['qty'], side, price, reversal)
            new_status = 'ENTERED'
            mtm_tracking = position_map['meta_data']['mtm_tracking']
            position_map['meta_data']['stop_loss'] = self.get_stoploss(mtm_tracking[1], mtm_tracking[-1])
            if not reversal:
                position_map['meta_data']['entry_count'] += 1
        elif status == 'ENTERED' and should_exit_check():
            for shadow_position in position_map['positions']:
                pos = await Position.filter(
                    active=True, subscription=sub, instrument_id=shadow_position['inst_id'], side=side
                ).select_related('instrument').get_or_none()
                if pos:
                    price = await self.get_current_price(pos.instrument)
                    await self.exit(pos, price)
            new_status = 'EXITED'
            position_map['meta_data']['stop_loss'] = None
            if not reversal:
                position_map['meta_data']['exit_count'] += 1
                position_map['meta_data']['is_on_going'] = False
        if status == new_status:
            changed = False
        else:
            changed = True
        return (new_status, changed)

    async def process_trades(self, sub: Subscription, position_map: PositionMap) -> Tuple[Status, bool]:
        investment = position_map['meta_data']['investment']
        mtm_tracking = position_map['meta_data']['mtm_tracking']
        mtm = mtm_tracking[-1]
        count = len(position_map['positions'])
        days_high = max(mtm_tracking)
        entry_count = position_map['meta_data']['entry_count']
        exit_count = position_map['meta_data']['exit_count']
        start_mtm = max(mtm_tracking[0], mtm_tracking[1])
        reset_mtm = mtm - start_mtm
        is_on_going = position_map['meta_data']['is_on_going']
        status = position_map['meta_data']['normal_status']
        status, changed = await self.process(
            status,
            lambda: self.should_enter(investment, mtm, count, days_high, entry_count, reset_mtm),
            lambda: self.should_exit(investment, mtm, count, days_high, exit_count, is_on_going, reset_mtm),
            sub, position_map
        )
        if changed:
            position_map['meta_data']['normal_status'] = status
        return (status, changed)
    
    async def process_trades_exit_only(self, sub: Subscription, position_map: PositionMap) -> Tuple[Status, bool]:
        investment = position_map['meta_data']['investment']
        mtm_tracking = position_map['meta_data']['mtm_tracking']
        mtm = mtm_tracking[-1]
        count = len(position_map['positions'])
        days_high = max(mtm_tracking)
        exit_count = position_map['meta_data']['exit_count']
        start_mtm = max(mtm_tracking[0], mtm_tracking[1])
        reset_mtm = mtm - start_mtm
        is_on_going = position_map['meta_data']['is_on_going']
        status = position_map['meta_data']['normal_status']
        status, changed = await self.process(
            status,
            lambda: False,
            lambda: self.should_exit(investment, mtm, count, days_high, exit_count, is_on_going, reset_mtm),
            sub, position_map
        )
        if changed:
            position_map['meta_data']['normal_status'] = status
        return (status, changed)

    async def process_reversal(self, sub: Subscription, position_map: PositionMap) -> Tuple[Status, bool]:
        investment = position_map['meta_data']['investment']
        mtm_tracking = position_map['meta_data']['mtm_tracking']
        mtm = mtm_tracking[-1]
        count = len(position_map['positions'])
        opposite_count = 10            ## Change this
        start_mtm = max(mtm_tracking[0], mtm_tracking[1])
        reset_mtm = mtm - start_mtm
        stop_loss = position_map['meta_data']['stop_loss']
        status = position_map['meta_data']['reversal_status']
        status, changed = await self.process(
            status,
            lambda: self.should_reverse(investment, mtm, reset_mtm, count, opposite_count),
            lambda: self.should_exit_reverse(mtm, reset_mtm) or self.sl_hit(stop_loss, mtm),
            sub, position_map,
            reversal=True
        )
        if changed:
            position_map['meta_data']['reversal_status'] = status
        return (status, changed)

    async def process_reversal_exit_only(self, sub: Subscription, position_map: PositionMap) -> Tuple[Status, bool]:
        mtm_tracking = position_map['meta_data']['mtm_tracking']
        mtm = mtm_tracking[-1]
        start_mtm = max(mtm_tracking[0], mtm_tracking[1])
        reset_mtm = mtm - start_mtm
        status = position_map['meta_data']['reversal_status']
        status, changed = await self.process(
            status,
            lambda: False,
            lambda: self.should_exit_reverse(mtm, reset_mtm),
            sub, position_map,
            reversal=True
        )
        if changed:
            position_map['meta_data']['reversal_status'] = status
        return (status, changed)

    async def process_sl_window(self, sub: Subscription, position_map: PositionMap) -> Tuple[Status, bool]:
        investment = position_map['meta_data']['investment']
        mtm_tracking = position_map['meta_data']['mtm_tracking']
        mtm = mtm_tracking[-1]
        count = len(position_map['positions'])
        days_high = max(mtm_tracking)
        entry_count = position_map['meta_data']['entry_count']
        exit_count = position_map['meta_data']['exit_count']
        start_mtm = max(mtm_tracking[0], mtm_tracking[1])
        reset_mtm = mtm - start_mtm
        is_on_going = position_map['meta_data']['is_on_going']
        stop_loss = position_map['meta_data']['stop_loss']
        status = position_map['meta_data']['sl_window_status']
        status, changed = await self.process(
            status,
            lambda: self.should_enter_with_sl(investment, mtm, count, days_high, entry_count, reset_mtm, start_mtm),
            lambda: (
                self.should_exit(investment, mtm, count, days_high, exit_count, is_on_going, reset_mtm)
                or self.sl_hit(stop_loss, mtm)
            ),
            sub, position_map
        )
        if changed:
            position_map['meta_data']['sl_window_status'] = status
        return (status, changed)

    async def process_sl_window_exit_only(self, sub: Subscription, position_map: PositionMap) -> Tuple[Status, bool]:
        investment = position_map['meta_data']['investment']
        mtm_tracking = position_map['meta_data']['mtm_tracking']
        mtm = mtm_tracking[-1]
        count = len(position_map['positions'])
        days_high = max(mtm_tracking)
        exit_count = position_map['meta_data']['exit_count']
        start_mtm = max(mtm_tracking[0], mtm_tracking[1])
        reset_mtm = mtm - start_mtm
        is_on_going = position_map['meta_data']['is_on_going']
        stop_loss = position_map['meta_data']['stop_loss']
        status = position_map['meta_data']['normal_status']
        status, changed = await self.process(
            status,
            lambda: False,
            lambda: (
                self.should_exit(investment, mtm, count, days_high, exit_count, is_on_going, reset_mtm)
                or self.sl_hit(stop_loss, mtm)
            ),
            sub, position_map
        )
        if changed:
            position_map['meta_data']['normal_status'] = status
        return (status, changed)
    
    async def process_exit_all(self, sub: Subscription, position_map: PositionMap) -> Tuple[Status, bool]:
        status = position_map['meta_data']['normal_status']
        status, changed = await self.process(
            status,
            lambda: False,
            lambda: True,
            sub, position_map
        )
        if changed:
            position_map['meta_data']['normal_status'] = status
        return (status, changed)

    async def process_exit_reversal(self, sub: Subscription, position_map: PositionMap) -> Tuple[Status, bool]:
        status = position_map['meta_data']['reversal_status']
        status, changed = await self.process(
            status,
            lambda: False,
            lambda: True,
            sub, position_map
        )
        if changed:
            position_map['meta_data']['reversal_status'] = status
        return (status, changed)

    async def process_exit_sl_window(self, sub: Subscription, position_map: PositionMap) -> Tuple[Status, bool]:
        status = position_map['meta_data']['normal_status']
        status, changed = await self.process(
            status,
            lambda: False,
            lambda: True,
            sub, position_map
        )
        if changed:
            position_map['meta_data']['normal_status'] = status
        return (status, changed)

    async def fno_ban_transform(self, position_map: PositionMap) -> PositionMap:
        stock_names = await FnOBanCheck.get_fno_ban_list()
        filtered_positions = []
        for shadow_position in position_map['positions']:
            instrument = await Instrument.filter(
                id=shadow_position['inst_id']
            ).select_related('future__stock').get()
            if instrument.future.stock.name not in stock_names:
                filtered_positions.append(shadow_position)
        position_map['positions'] = filtered_positions
        return position_map

    async def results_transform(self, position_map: PositionMap) -> PositionMap:
        stock_names = await ResultsHedgeAlgo.get_results_stock_names()
        filtered_positions = []
        for shadow_position in position_map['positions']:
            instrument = await Instrument.filter(
                id=shadow_position['inst_id']
            ).select_related('future__stock').get()
            if instrument.future.stock.name not in stock_names:
                filtered_positions.append(shadow_position)
        position_map['positions'] = filtered_positions
        return position_map

    async def base_strategy_entry_transform(self, position_map: PositionMap, account: Account) -> PositionMap:
        stock_calls = await self.generate_stock_calls()
        now = datetime.datetime.now()
        today = now.date()
        new_shadow_positions = []
        inst_ids = set(pos['inst_id'] for pos in position_map['positions'])
        for stock, side in stock_calls.items():
            if side == TradeSide(position_map['side']):
                instrument = await Instrument.filter(
                    future__stock=stock,
                    future__expiry__gt=today 
                ).order_by('future__expiry').first()
                if instrument.id not in inst_ids:
                    price = await self.get_current_price(instrument)
                    qty = await self.get_qty(instrument, account)
                    new_shadow_positions.append({
                        'inst_id': instrument.id,
                        'price': float(price),
                        'side': side.value,
                        'qty': int(qty),
                        'entry_time': now.isoformat(),
                        'old_price': float(price),
                        'mtm': 0.0
                    })
        position_map['positions'] += new_shadow_positions
        return position_map

    async def base_strategy_exit_transform(self, position_map: PositionMap) -> PositionMap:
        stock_calls = await self.generate_stock_calls()
        now = datetime.datetime.now()
        today = now.date()
        for shadow_position in position_map['positions']:
            exit_time = shadow_position.get('exit_time')
            instrument = await Instrument.filter(
                id=shadow_position['inst_id']
            ).select_related('future__stock').get()
            if exit_time and datetime.datetime.fromisoformat(exit_time).date() < today:
                continue
            if TradeSide(shadow_position['side']) != stock_calls.get(instrument.future.stock):
                shadow_position['exit_time'] = now.isoformat()
                price = await self.get_current_price(instrument)
                shadow_position['exit_price'] = price
                await self.update_shadow_position_mtm(shadow_position)
        return position_map
    
    async def enter_trades(self, position_map: PositionMap, sub: Subscription):
        if position_map['meta_data'].get('normal_status') != 'ENTERED':
            return
        for shadow_position in position_map['positions']:
            pos = await Position.filter(
                active=True, subscription=sub, instrument_id=shadow_position['inst_id']
            ).select_related('instrument').get_or_none()
            if pos and (pos.side != TradeSide(shadow_position['side']) or shadow_position.get('exit_time')):
                price = await self.get_current_price(pos.instrument)
                await self.exit(pos, price)
            if not pos or (pos and pos.side != TradeSide(shadow_position['side'])):
                instrument = await Instrument.filter(id=shadow_position['inst_id']).get()
                price = await self.get_current_price(instrument)
                self.entry(sub, instrument, shadow_position['qty'], TradeSide(shadow_position['side']), price)
            
    async def update_mtm_transform(self, position_map: PositionMap) -> PositionMap:
        mtm = 0
        for shadow_position in position_map['positions']:
            await self.update_shadow_position_mtm(shadow_position)
            mtm += shadow_position['mtm']
        position_map['meta_data']['mtm_tracking'].append(mtm)
        return position_map

    async def shadow_split_transform(self, position_map: PositionMap):
        meta_data = position_map['meta_data']
        if (
            meta_data['normal_status'] == 'ENTERED'
            and meta_data['mtm_tracking'][-1] > 0
            and not meta_data['splitted']
        ):
            meta_data['splitted'] = True
            new_position_map = position_map.copy()
            new_position_map['positions'] = []
            new_position_map['meta_data']['splitted'] = False
            return [position_map, new_position_map]
        return [position_map]
    
    async def shadow_join_transform(self, position_maps: List[PositionMap]):
        if len(set([posm['side'] for posm in position_maps])) > 1:
            raise ValueError()
        statuses = [posm['meta_data']['normal_status'] for posm in position_maps]
        # 7.5 lakh exit
        if all(map(lambda status: status == 'EXITED', statuses)):
            position_map = position_maps[0].copy()
            position_map['meta_data']['splitted'] = False
            position_map['positions'] = []
            return position_map
        
    async def reset_meta_data(self, position_map: PositionMap, sub: Subscription) -> PositionMap:
        meta_data = position_map['meta_data']
        meta_data['entry_count'] = 0
        meta_data['exit_count'] = 0
        meta_data['investment'] = float(await Investment.filter(
            account=sub.account
        ).annotate(sum=Sum('amount')).first().values_list('sum', flat=True))
        meta_data['normal_status'] = meta_data.get('normal_status', 'EXITED')
        meta_data['is_on_going'] = meta_data['normal_status'] == 'ENTERED'
        meta_data['mtm_tracking'] = []
        meta_data['opposite_count'] = 0
        meta_data['reversal_status'] = meta_data.get('reversal_status', 'EXITED')
        meta_data['sl_window_status'] = meta_data.get('sl_window_status', 'EXITED')
        meta_data['splitted'] = meta_data.get('splitted', False)
        meta_data['stop_loss'] = meta_data.get('stop_loss')
        position_map['meta_data'] = meta_data
        position_map['trade_baskets'] = {}
        return position_map

    async def trade_baskets_create(self, position_map: PositionMap, sub: Subscription) -> Dict[str, List[Trade]]:
        get_opposite_side = lambda side: TradeSide.BUY if TradeSide(side) == TradeSide.SELL else TradeSide.SELL
        new_exits = []
        new_entrys = []
        all_entrys = []
        for shadow_position in position_map['positions']:
            pos = await Position.filter(
                subscription=sub,
                active=True,
                instrument_id=shadow_position['inst_id'],
                side=TradeSide(shadow_position['side'])
            ).select_related('instrument').get_or_none()
            if shadow_position.get('exit_time'):
                if pos and pos.side == TradeSide(shadow_position['side']):
                    price = await self.get_current_price(pos.instrument)
                    trade = Trade(
                        subscription=sub,
                        instrument=pos.instrument,
                        qty=pos.qty,
                        price=price,
                        side=get_opposite_side(pos.side)
                    )
                    new_exits.append(trade)
            else:
                instrument = await Instrument.filter(id=shadow_position['inst_id']).get()
                price = await self.get_current_price(instrument)
                qty = await self.get_qty(instrument, sub.account)
                trade = Trade(
                    subscription=sub,
                    instrument=instrument,
                    qty=qty,
                    price=price,
                    side=TradeSide(shadow_position['side'])
                )
                all_entrys.append(trade)
            if not pos and not shadow_position.get('exit_time'):
                new_entrys.append(trade)
        all_exits = all_entrys.copy()
        for trade in all_exits:
            trade.side = get_opposite_side(trade.side)
        return {
            '9_45_entrys': new_entrys,
            '9_45_exits': new_exits,
            'all_entrys': all_entrys,
            'all_exits': all_exits,
            'reversal_entrys': all_exits,
            'reversal_exits': all_entrys,
        }

    async def trigger_emails(self, mailer: TradeSplitMailer, subject_tag: str = ""):
        await mailer.create_trades_mails(self.trades, subject_tag)
        self.trades = []

    async def run(self):
        subs = await Subscription.filter(active=True, algo=self.algo).select_related('account')
        default_position_maps: List[PositionMap] = [
            {
                'positions': [],
                'meta_data': {},
                'trade_baskets': {}, 
                'side': TradeSide.BUY.value,
            },
            {
                'positions': [],
                'meta_data': {},
                'trade_baskets': {},
                'side': TradeSide.SELL.value,
            }
        ]
        mailer = TradeSplitMailer()
        for sub in subs:
            sub_data, _ = await SubscriptionData.get_or_create(subscription=sub, defaults=dict(data={
                'position_maps': default_position_maps
            }))
            position_maps: List[PositionMap] = sub_data.data['position_maps']
            for position_map in position_maps:
                if self.action == '9_20':
                    await self.reset_meta_data(position_map, sub)
                position_map = await self.update_mtm_transform(position_map)
                if self.action == '9_20':
                    position_map = await self.base_strategy_exit_transform(position_map)
                    position_map = await self.results_transform(position_map)
                    position_map = await self.fno_ban_transform(position_map)
                    self.trigger_emails(mailer)
                    if not position_map['meta_data']['splitted']:
                        position_map = await self.base_strategy_entry_transform(position_map, sub.account)
                elif self.action == '9_30':
                    trade_baskets = await self.trade_baskets_create(position_map, sub)
                    await mailer.create_baskets_mail([sub.account], trade_baskets)
                elif self.action == '9_45':
                    self.enter_trades(position_map, sub)
                    self.trigger_emails(mailer, subject_tag="Ongoing")
                elif self.action in ['9_45', '10_to_2_15']:
                    status, changed = await self.process_reversal(sub, position_map)
                    if changed:
                        self.trigger_emails(mailer, subject_tag="Reversal")
                    if status != 'ENTERED':
                        status, changed = await self.process_trades(sub, position_map)
                        if changed:
                            self.trigger_emails(mailer)
                    if status != 'ENTERED':
                        status, changed = await self.process_sl_window(sub, position_map)
                        if changed:
                            self.trigger_emails(mailer, subject_tag="StopLoss")
                elif self.action == '2_30_to_3':
                    status, changed = await self.process_reversal_exit_only(sub, position_map)
                    if changed:
                        self.trigger_emails(mailer, subject_tag="Reversal")
                    status, changed = await self.process_sl_window_exit_only(sub, position_map)
                    if changed:
                        self.trigger_emails(mailer, subject_tag="StopLoss")
                    status, changed = await self.process_trades_exit_only(sub, position_map)
                    if changed:
                        self.trigger_emails(mailer)
                elif self.action == '3_15':
                    _, changed = await self.process_exit_reversal(sub, position_map)
                    if changed:
                        self.trigger_emails(mailer, subject_tag="Reversal")
                    _, changed = await self.process_exit_sl_window(sub, position_map)
                    if changed:
                        self.trigger_emails(mailer, subject_tag="StopLoss")
                    position_map = await self.base_strategy_exit_transform(position_map)
                    self.enter_trades(position_map, sub)
                    self.trigger_emails(mailer)
            if self.action == '3_20':
                long_posms: List[PositionMap] = [posm for posm in position_maps if TradeSide(posm['side']) == TradeSide.BUY]
                short_posms: List[PositionMap] = [posm for posm in position_maps if TradeSide(posm['side']) == TradeSide.SELL]
                long_mtm_sum = sum(posm['meta_data']['mtm_tracking'][-1] for posm in long_posms)
                short_mtm_sum = sum(posm['meta_data']['mtm_tracking'][-1] for posm in short_posms)
                if long_mtm_sum > 750000:
                    for position_map in long_posms:
                        await self.process_exit_all(sub, position_map)
                if short_mtm_sum > 750000:
                    for position_map in short_posms:
                        await self.process_exit_all(sub, position_map)
                if any(map(lambda posm: posm['meta_data']['splitted'], position_maps)):
                    # join
                    long_posm = await self.shadow_join_transform(long_posms)
                    short_posm = await self.shadow_join_transform(short_posms)
                    position_maps = [long_posm, short_posm]
                elif not any(posm['meta_data']['splitted'] for posm in position_maps):
                    # split
                    position_maps_splitted = [self.shadow_split_transform(posm) for posm in position_maps]
                    position_maps_splitted = await asyncio.gather(*position_maps_splitted)
                    position_maps = list(itertools.chain(*position_maps_splitted))
            sub_data.data['position_maps'] = position_maps
            await sub_data.save()