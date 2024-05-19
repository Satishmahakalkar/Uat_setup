import datetime
import importlib
from typing import Optional
from mangum import Mangum
from tortoise import Tortoise
from accounts.googlesheet import GoogleSheetEdit
from accounts.execute import SRETradeExecutor
from accounts.mail import PnlMailer, PositionsMailer, ShadowPositionsMailer, ShadowTradeBasketMailer, TradesMailer
from accounts.pnl import PnlSave
from accounts.killswitch import exit_all_trades, exit_trades_for_account
from algos.basealgo import BaseAlgo
from algos.componentanalysis import ComponentAnalysis
from algos.tradecountstopper import TradeCountStopper
from dataaggregator.truedata.datasaver import TrueData
from database.models import Account
from apiserver.app import make_app
import settings
import asyncio
import logging

logging.basicConfig(handlers=[logging.StreamHandler()], level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)


class LambdaExecutor:

    def __init__(self, lambda_event: dict) -> None:
        self.lambda_event = lambda_event

    async def init(self):
        logging.info("Db connection init")
        await Tortoise.init(settings.TORTOISE_ORM)

    async def action_regular_or_rectification(self):
        now = datetime.datetime.utcnow()
        if now.time() < datetime.time(hour=5, minute=1):
            return { 'regular_or_rectification': 'RECTIFICATION' }
        else:
            return { 'regular_or_rectification': 'REGULAR' }

    async def action_is_holiday(self):
        today = datetime.date.today()
        if today.weekday() in (6, 5):
            is_holiday = True
        elif today in settings.HOLIDAY_DATES:
            is_holiday = True
        else:
            is_holiday = False
        return { 'is_holiday': is_holiday }

    async def action_truedatasave(self):
        data_saver = TrueData()
        logging.info("truedata")
        await data_saver.login()
        logging.info("logged in")
        await data_saver.save_historical_data_for_stocks()
        await data_saver.save_historical_data_for_futures()

    async def action_truedataltpsave(self, **kwargs):
        data_saver = TrueData()
        await data_saver.login()
        # await data_saver.save_historical_data_ltp()
        await data_saver.save_ltp_all(**kwargs)

    async def action_run_algo(self, algo_name: str, mailer=True, send_no_trades=True, reversal_mail=False, partial_mail=False, **kwargs):
        module = importlib.import_module(f'algos.{algo_name.lower()}')
        algo_strat_class = getattr(module, algo_name)
        algo_strat: BaseAlgo = algo_strat_class()
        await algo_strat.init(**kwargs)
        await algo_strat.run()
        if mailer:
            mailer = TradesMailer(algo_strat, send_no_trades, reverse=reversal_mail, partial=partial_mail)
            await mailer.run()
        sre_trade_executor = SRETradeExecutor()
        await sre_trade_executor.save_trades(algo_strat.trades)

    async def action_rollover(self, algo_name: str):
        module = importlib.import_module(f'algos.{algo_name.lower()}')
        algo_strat_class = getattr(module, algo_name)
        algo_strat: BaseAlgo = algo_strat_class()
        await algo_strat.init()
        await algo_strat.rollover()
        mailer = TradesMailer(algo_strat, send_no_trades=False, rollover=True)
        await mailer.run()

    async def action_pnlsave(self):
        pnl_saver = PnlSave()
        await pnl_saver.run()
        pnl_mailer = PnlMailer()
        await pnl_mailer.run()

    async def action_send_positions(self):
        pnl_saver = PnlSave()
        await pnl_saver.save_eod_price()
        positions_mailer = PositionsMailer()
        await positions_mailer.run()

    async def action_populate_instruments(self):
        data_saver = TrueData()
        await data_saver.login()
        await data_saver.populate_instruments()

    async def action_shadow_sheet(self, futures_price_only=False, append_mtms=False):
        gs = GoogleSheetEdit()
        await gs.init()
        if not futures_price_only:
            await gs.update_shadow_positions()
        if append_mtms:
            await gs.append_shadow_mtms()
        await gs.update_futures_prices()

    async def action_exit_all_trades(self):
        await exit_all_trades()

    async def action_exit_trades_for_account(self, account_name):
        account = await Account.get(name=account_name)
        await exit_trades_for_account(account)

    async def action_place_sre_trades(self):
        sre_trade_executor = SRETradeExecutor()
        await sre_trade_executor.execute_trades()

    async def action_check_sre_trades(self):
        sre_trade_executor = SRETradeExecutor()
        await sre_trade_executor.check_trades()

    async def action_mail_trade_baskets(self):
        mailer = ShadowTradeBasketMailer()
        await mailer.run()

    async def action_trade_counter_calculate(self):
        algo = TradeCountStopper()
        await algo.init()
        await algo.run()
        gs = GoogleSheetEdit()
        await gs.init()
        await gs.update_trade_counter_ratios()

    async def action_component_analysis(self):
        algo = ComponentAnalysis()
        await algo.init()
        await algo.run()
        gs = GoogleSheetEdit()
        await gs.init()
        await gs.component_analysis()

    async def run(self):
        await self.init()
        action = self.lambda_event['action']
        kwargs = self.lambda_event.get('kwargs', {})
        logging.info(f"Action {action}, {kwargs}")
        method = getattr(self, f"action_{action}")
        logging.info(f"method {method.__func__.__name__}")
        return await method(**kwargs)


def lambda_handler(event, context):
    lmb = LambdaExecutor(event)
    loop = asyncio.get_event_loop()
    result: dict = loop.run_until_complete(lmb.run())
    logging.info(f"Result of process {result}")
    if isinstance(result, dict):
        result.update({ 'success': True })
    else:
        result = { 'success': True }
    return result


api_server_handler = Mangum(make_app())