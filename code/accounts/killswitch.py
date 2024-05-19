import datetime
import importlib
from typing import Optional
from accounts.mail import TradesMailer
from algos.basealgo import BaseAlgo, BaseAlgoPnlRMS
from algos.shadowanalysis import ShadowAnalysis
from database.models import Account, Algo, Instrument, Ltp, Position, Subscription, SubscriptionData, Trade, TradeExit, TradeSide
from tortoise.expressions import Subquery


async def exit_all_trades(side: Optional[TradeSide] = None):
    algos = await Algo.filter(id__in=Subquery(Subscription.filter(active=True).values('algo_id')))
    for algo in algos:
        module = importlib.import_module(f'algos.{algo.name.lower()}')
        algo_strat_class = getattr(module, algo.name)
        algo_strat: BaseAlgo = algo_strat_class()
        positions_q = Position.filter(
            subscription__active=True, subscription__algo=algo, active=True
        ).select_related('instrument')
        if side:
            positions = await positions_q.filter(side=side)
        else:
            positions = await positions_q
        for position in positions:
            ltp = await Ltp.filter(instrument=position.instrument).get()
            await algo_strat.exit(position, ltp.price)
        mailer = TradesMailer(algo_strat, send_no_trades=False)
        await mailer.run()


async def delete_trades_for_date(account: Account, date: datetime.date):
    trade_exit_q = TradeExit.filter(
        entry_trade__subscription__account=account,
        entry_trade__subscription__active=True,
        entry_trade__timestamp__gte=date,
        entry_trade__timestamp__lt=(date + datetime.timedelta(days=1))
    )
    trade_ids = await trade_exit_q.values_list('entry_trade_id', flat=True)
    position_ids = await trade_exit_q.values_list('position_id', flat=True)
    trade_exit_ids = await trade_exit_q.values_list('id', flat=True)
    await Trade.filter(id__in=trade_ids).delete()
    await Position.filter(id__in=position_ids).delete()
    await TradeExit.filter(id__in=trade_exit_ids).delete()


async def exit_trades_for_account(account: Account, side: Optional[TradeSide] = None, killswitch=False, mails=False):
    subscriptions = await Subscription.filter(account=account, active=True, is_hedge=False).select_related('algo', 'account')
    for sub in subscriptions:
        module = importlib.import_module(f'algos.{sub.algo.name.lower()}')
        algo_strat_class = getattr(module, sub.algo.name)
        algo_strat: BaseAlgo = algo_strat_class()
        await algo_strat.init()
        positions_q = Position.filter(subscription=sub, active=True).select_related('instrument')
        sub_data = await SubscriptionData.filter(subscription=sub).get()
        if side:
            positions = await positions_q.filter(side=side)
            if side == TradeSide.BUY:
                sub_data.data['long_kill_switch'] = killswitch
            else:
                sub_data.data['short_kill_switch'] = killswitch
        else:
            sub_data.data['long_kill_switch'] = killswitch
            sub_data.data['short_kill_switch'] = killswitch
            positions = await positions_q
        for position in positions:
            ltp = await Ltp.filter(instrument=position.instrument).get()
            await algo_strat.exit(position, ltp.price)
        sub_data.data['shadow_long_status'] = "EXITED"
        sub_data.data['shadow_short_status'] = "EXITED"
        await sub_data.save()
        if mails:
            mailer = TradesMailer(algo_strat, send_no_trades=False)
            await mailer.run()


async def send_trades_from_shadow(account: Account, side: Optional[TradeSide] = None):
    subscriptions = await Subscription.filter(account=account, active=True, is_hedge=False).select_related('algo', 'account')
    for sub in subscriptions:
        module = importlib.import_module(f'algos.{sub.algo.name.lower()}')
        algo_strat_class = getattr(module, sub.algo.name)
        algo_strat: BaseAlgoPnlRMS = algo_strat_class()
        await algo_strat.init()
        sub_data = await SubscriptionData.filter(subscription=sub).get()
        stored_positions = sub_data.data.get('positions', [])
        active_insts = await Position.filter(
            active=True,
            subscription=sub
        ).values_list('instrument_id', flat=True)
        today = datetime.date.today()
        for values in stored_positions:
            if not values.get('exit_time') and values['inst_id'] not in active_insts:
                if side and TradeSide(values['side']) != side:
                    continue
                instrument = await Instrument.get(id=values['inst_id'])
                ltp = await Ltp.filter(instrument=instrument).get()
                await algo_strat.entry(sub, instrument, values['qty'], TradeSide(values['side']), ltp.price)
            elif values.get('exit_time') and values['inst_id'] in active_insts:
                position = await Position.filter(subscription=sub, active=True, instrument_id=values['inst_id']).get()
                exit_date = datetime.datetime.fromisoformat(values['exit_time']).date()
                if position.side == TradeSide(values['side']) and exit_date == today:
                    ltp = await Ltp.filter(instrument=instrument).get()
                    await algo_strat.exit(position, ltp.price)
        if side and side == TradeSide.BUY:
            sub_data.data['long_kill_switch'] = False
        elif side and side == TradeSide.SELL:
            sub_data.data['short_kill_switch'] = False
        elif not side:
            sub_data.data['long_kill_switch'] = False
            sub_data.data['short_kill_switch'] = False
        await sub_data.save()
        mailer = TradesMailer(algo_strat, send_no_trades=False)
        await mailer.run()


async def reverse_trades(account: Account, side: Optional[TradeSide]):
    sub = await Subscription.filter(account=account, active=True, is_hedge=False).select_related('algo', 'account').get()
    module = importlib.import_module(f'algos.{sub.algo.name.lower()}')
    algo_strat_class = getattr(module, sub.algo.name)
    assert issubclass(algo_strat_class, ShadowAnalysis)
    sub_data = await SubscriptionData.filter(subscription=sub).get()
    algo_strat: ShadowAnalysis = algo_strat_class()
    await algo_strat.init()
    await algo_strat.enter_reverse_from_shadow(sub_data, side)
    sub_data.data['shadow_long_status'] = "REVERSED"
    await sub_data.save()
    mailer = TradesMailer(algo_strat, send_no_trades=False, reverse=True)
    await mailer.run()


async def reverse_trade_exit(account: Account, side: Optional[TradeSide]):
    sub = await Subscription.filter(account=account, active=True, is_hedge=False).select_related('algo', 'account').get()
    module = importlib.import_module(f'algos.{sub.algo.name.lower()}')
    algo_strat_class = getattr(module, sub.algo.name)
    assert issubclass(algo_strat_class, ShadowAnalysis)
    sub_data = await SubscriptionData.filter(subscription=sub).get()
    algo_strat: ShadowAnalysis = algo_strat_class()
    await algo_strat.init()
    await algo_strat.exit_reversed(sub_data, side)
    sub_data.data['shadow_long_status'] = "EXITED"
    mailer = TradesMailer(algo_strat, send_no_trades=False, reverse=True)
    await mailer.run()