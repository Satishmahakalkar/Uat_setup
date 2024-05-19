import datetime
import unittest
from tortoise import Tortoise, run_async
from tortoise.contrib import test
from accounts.pnl import PnlSave
from accounts.seeddata import Seed
from algos.niftyfuturesalgo import NiftyFuturesAlgo
from dataaggregator.truedata.datasaver import TrueData
from database.models import Account, Algo, Future, Instrument, Interval, Investment, Ltp, Ohlc, PnL, Position, SREAccount, Stock, StockGroup, StockGroupMap, Strategy, Subscription, Trade, TradeExit, TradeSide, User
from main import lambda_handler


class TruedataTest(test.TestCase):

    async def _setUp(self) -> None:
        self.stock, _ = await Stock.get_or_create(ticker='TCS', isin="test3", name='TCS')
        self.instrument, _ = await Instrument.get_or_create(stock=self.stock, future=None, option=None)
        self.data_saver = TrueData()
        await self.data_saver.login()

    def setUp(self) -> None:
        test.initializer(["database.models"], app_label="models")
        run_async(self._setUp())

    def tearDown(self) -> None:
        test.finalizer()

    async def test_ltpsave(self):
        future = await Future.create(stock=self.stock, expiry=datetime.date(2023,9,28), lot_size=1)
        instrument = await Instrument.create(future=future)
        await self.data_saver.save_ltp_all()
        ltp = await Ltp.filter(instrument=self.instrument).get_or_none()
        self.assertIsInstance(ltp, Ltp)
        self.assertIsInstance(ltp.price, float)
        self.assertIsInstance(ltp.timestamp, datetime.datetime)
        ltp = await Ltp.filter(instrument=instrument).get_or_none()
        self.assertIsInstance(ltp, Ltp)
        self.assertIsInstance(ltp.price, float)
        self.assertIsInstance(ltp.timestamp, datetime.datetime)

    async def test_historical_data(self):
        sg = await StockGroup.create(name="test")
        await StockGroupMap.create(stock_group=sg, stock=self.stock)
        await self.data_saver.save_historical_data_for_stocks()
        ohlc_count = await Ohlc.filter(instrument=self.instrument).count()
        self.assertEqual(ohlc_count, 365)

    async def test_populate_data(self):
        await self.stock.delete()
        await self.instrument.delete()
        await self.data_saver.populate_instruments()
        stock = await Stock.filter(ticker='TCS').get_or_none()
        self.assertIsInstance(stock, Stock)
        instrument = await Instrument.filter(stock=stock).get_or_none()
        self.assertIsInstance(instrument, Instrument)


class AlgoTest(test.TestCase):

    async def _setUp(self):
        algo = await Algo.create(name="NiftyFuturesAlgo")
        strategy = await Strategy.create(name='strategy2')
        stock_group = await StockGroup.create(name='Nifty50')
        stock = await Stock.create(ticker='TCS', name='TCS', isin='test')
        await StockGroupMap.create(stock_group=stock_group, stock=stock)
        user = await User.create(email='test@test.com')
        account = await Account.create(user=user, start_date=datetime.date.today())
        subscription = await Subscription.create(account=account, algo=algo, start_date=datetime.date.today())
        future = await Future.create(stock=stock, expiry=datetime.date.today() + datetime.timedelta(days=1), lot_size=10)
        instrument_fut = await Instrument.create(stock=None, future=future, option=None)
        instrument_stock = await Instrument.create(stock=stock, future=None, option=None)
        investment = await Investment.create(account=account, amount=10000)
        investment = await Investment.create(account=account, amount=20000)
        ltp = await Ltp.create(instrument=instrument_stock, price=100)
        ltp = await Ltp.create(instrument=instrument_fut, price=120)
        for i in range(365):
            await Ohlc.create(
                instrument=instrument_stock,
                timestamp=datetime.datetime.now(),
                interval=Interval.EOD,
                open=i,
                high=(i+10),
                low=(i-10),
                close=i
            )

    def setUp(self) -> None:
        test.initializer(["database.models"], app_label="models")
        run_async(self._setUp())

    def tearDown(self) -> None:
        test.finalizer()

    async def test_run(self):
        algo = NiftyFuturesAlgo()
        await algo.init()
        await algo.run()
        td = algo.trades[0]
        self.assertIsInstance(td, Trade)
        self.assertEqual(td.side, TradeSide.BUY)
        pos = await Position.get_or_none()
        self.assertIsInstance(pos, Position)
        self.assertTrue(pos.active)
        await pos.fetch_related('instrument')
        await td.fetch_related('instrument')
        self.assertEqual(td.instrument, pos.instrument)
        tde = await TradeExit.all().select_related('entry_trade', 'exit_trade', 'position').get_or_none()
        self.assertEqual(tde.entry_trade, td)
        self.assertEqual(tde.position, pos)
        self.assertIsNone(tde.exit_trade)
        account = await Account.get(id=1)
        await PnlSave(account).save_pnl()
        pnl = await PnL.get_or_none(account=account)
        self.assertIsInstance(pnl, PnL)

    async def test_double_run(self):
        algo = NiftyFuturesAlgo()
        await algo.init()
        await algo.run()
        algo = NiftyFuturesAlgo()
        await algo.init()
        await algo.run()
        td = await Trade.get_or_none()
        self.assertIsInstance(td, Trade)
        self.assertEqual(td.side, TradeSide.BUY)
        pos = await Position.get_or_none()
        self.assertIsInstance(pos, Position)
        await pos.fetch_related('instrument')
        await td.fetch_related('instrument')
        self.assertEqual(td.instrument, pos.instrument)
        tde = await TradeExit.all().select_related('entry_trade', 'exit_trade', 'position').get_or_none()
        self.assertEqual(tde.entry_trade, td)
        self.assertEqual(tde.position, pos)
        self.assertIsNone(tde.exit_trade)
        account = await Account.get(id=1)
        await PnlSave(account).save_pnl()
        pnl = await PnL.get_or_none(account=account)
        self.assertIsInstance(pnl, PnL)



class SeedTest(test.TestCase):

    def setUp(self) -> None:
        test.initializer(["database.models"], app_label="models")
        self.seed = Seed()

    def tearDown(self) -> None:
        test.finalizer()

    async def test_add_algo(self):
        await self.seed.add_algo("test")
        algo = await Algo.get_or_none(name="test")
        self.assertIsInstance(algo, Algo)

    async def test_add_strategy(self):
        await self.seed.add_strategy("test")
        strat = await Strategy.get_or_none(name="test")
        self.assertIsInstance(strat, Strategy)

    async def test_investment(self):
        user = await User.create(email="test@test.com")
        account = await Account.create(user=user, start_date=datetime.date.today())
        await self.seed.add_investment(account.id, 10000)
        investment = await Investment.get_or_none(account=account)
        self.assertIsInstance(investment, Investment)
        self.assertEqual(investment.amount, 10000)

    async def test_subscription(self):
        algo = await Algo.create(name="testalgo")
        await self.seed.add_subscription("test@test.com", "testalgo", False)
        sub = await Subscription.get_or_none(account__user__email="test@test.com")
        self.assertIsInstance(sub, Subscription)
        await sub.fetch_related('algo', 'account')
        self.assertEqual(sub.algo, algo)
        self.assertEqual(sub.algo.name, "testalgo")
        sre_account = await SREAccount.get_or_none(account=sub.account)
        self.assertIsNone(sre_account)
        await self.seed.add_subscription("test@test.com", "testalgo", True)
        sre_account = await SREAccount.get_or_none(account=sub.account)
        self.assertIsInstance(sre_account, SREAccount)

    async def test_nifty_stock_group(self):
        data_saver = TrueData()
        await data_saver.populate_instruments()
        await self.seed.save_nifty_stock_group()
        sg = await StockGroup.get_or_none(name='Nifty50')
        self.assertIsInstance(sg, StockGroup)
        number = await StockGroupMap.filter(stock_group=sg).count()
        self.assertEqual(number, 50)


class LambdaTest(test.TestCase):

    async def _setUp(self):
        self.seed = Seed()

    def setUp(self) -> None:
        test.initializer(["database.models"], app_label="models")
        run_async(self._setUp())

    def tearDown(self) -> None:
        test.finalizer()

    async def test_ltp_save(self):
        data_saver = TrueData()
        await data_saver.login()
        await data_saver.populate_instruments()
        await data_saver.save_ltp_all()
        ltp = await Ltp.filter(instrument__stock__ticker='ITC').get_or_none()
        self.assertIsInstance(ltp, Ltp)
        self.assertIsInstance(ltp.price, float)
        count = await Ohlc.filter(instrument__stock__ticker='ITC').count()
        self.assertEqual(count, 1)
