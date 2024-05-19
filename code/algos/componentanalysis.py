import datetime
from typing import List, Literal
from algos.basealgo import BaseAlgo
from algos.shadowanalysis import ShadowPosition
from database.models import Account, Algo, Ltp, Subscription, SubscriptionData, TradeSide
from tortoise.expressions import Subquery


class OldDateError(Exception):
    pass


class ShadowPositionCompAnalysis(ShadowPosition):
    action: Literal["ENTRY", "EXIT", "NOCHANGE"]
    exited_by_comp_analysis: bool


class ComponentAnalysis(BaseAlgo):

    async def init(self):
        self.algo = await Algo.get(name=self.__class__.__name__)

    async def run(self):
        subs = await Subscription.filter(algo=self.algo, active=True).select_related('account')
        base_min_move = 12000
        now = datetime.datetime.now()
        today = now.date()
        for sub in subs:
            sub_data, _ = await SubscriptionData.get_or_create(subscription=sub, defaults=dict(data={}))
            sub_data_main = await SubscriptionData.filter(subscription__account=sub.account, subscription__is_hedge=False).get_or_none()
            try:
                sync_date = datetime.date.fromisoformat(sub_data.data.get('sync_date'))
                if sync_date < today:
                    raise OldDateError
                stronger_side = sub_data.data['stronger_side']
                min_move_stock = sub_data.data.get('min_move_stock', base_min_move)
                if stronger_side == TradeSide.BUY:
                    mtm_tracking = sub_data_main.data.get('long_mtm_tracking')
                else:
                    mtm_tracking = sub_data_main.data.get('short_mtm_tracking')
                if len(mtm_tracking) < 2:
                    continue
                last, second_last = mtm_tracking[-1], mtm_tracking[-2]
                average_movement = (last - second_last) / base_min_move
                if average_movement > 2000:
                    new_min_move = min_move_stock + average_movement
                else:
                    new_min_move = min_move_stock + 2000
                sub_data.data['min_move_stock'] = new_min_move
            except (TypeError, ValueError, OldDateError, KeyError):
                if not sub_data_main:
                    continue
                sub_data.data['positions'] = sub_data_main.data.get('positions')
                long_count, short_count = 0, 0
                for shadow_position in sub_data.data['positions']:
                    if TradeSide(shadow_position['side']) == TradeSide.BUY:
                        long_count += 1
                    else:
                        short_count += 1
                if long_count >= short_count:
                    stronger_side = TradeSide.BUY
                else:
                    stronger_side = TradeSide.SELL
                sub_data.data['stronger_side'] = stronger_side.value
                sub_data.data['sync_date'] = today.isoformat()
                sub_data.data['min_move_stock'] = base_min_move
                new_min_move = base_min_move
            shadow_positions: List[ShadowPositionCompAnalysis] = sub_data.data.get('positions', [])
            for shadow_position in shadow_positions:
                ltp = await Ltp.filter(instrument_id=shadow_position['inst_id']).get()
                if TradeSide(shadow_position['side']) == TradeSide.BUY:
                    mtm = shadow_position['qty'] * (ltp.price - shadow_position.get('exit_price', shadow_position['old_price']))
                else:
                    mtm = shadow_position['qty'] * (shadow_position.get('exit_price', shadow_position['old_price']) - ltp.price)
                abs_mtm = abs(mtm)
                if not shadow_position.get('exit_time') and abs_mtm < new_min_move:
                    shadow_position['exit_time'] = now.isoformat()
                    shadow_position['exit_price'] = ltp.price
                    shadow_position['action'] = 'EXIT'
                    shadow_position['exited_by_comp_analysis'] = True
                elif shadow_position.get('exit_time') and shadow_position.get('exited_by_comp_analysis') and abs_mtm > new_min_move:
                    shadow_position.pop('exit_time')
                    shadow_position.pop('exit_price')
                    shadow_position.pop('exited_by_comp_analysis')
                    shadow_position['action'] = 'ENTRY'
                else:
                    shadow_position['action'] = 'NOCHANGE'
                shadow_position['mtm'] = mtm
            sub_data.data['positions'] = shadow_positions
            await sub_data.save()
            
