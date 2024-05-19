import datetime
import logging
from typing import List, Literal
import aiohttp
from accounts.mail import SRETradesMailer
from database.models import Instrument, Ltp, SREAccount, SREOrders, Trade, TradeSide
from dataaggregator.sre.datasaver import SREMarketData
from tortoise.expressions import Subquery
import settings


class SREExecute:

    def __init__(self, sre_account: SREAccount) -> None:
        self._access_token = None
        self._api_root = "https://xts.sre.co.in"
        self._client_id = "*****"
        self.market_api = SREMarketData()
        self.sre_account = sre_account
        ##
        #  In case of multiple accounts with SRE we will need a user id stored in db 
        # or store different secrets according to accounts.
        ##

    @property
    def access_token(self):
        if not self._access_token:
            raise ValueError("Not logged in")
        return self._access_token
    
    async def login(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self._api_root}/interactive/user/session", json={
                'secretKey': settings.XTS_INTERACTIVE_APP_SECRET,
                'appKey': settings.XTS_INTERACTIVE_APP_KEY,
                'source': 'WebAPI'
            }) as res:
                if not res.ok:
                    raise ValueError("Login failed")
                data = await res.json()
                self._access_token = data['result']['token']
        await self.market_api.login()
        await self.market_api.get_instrument_master()

    async def logout(self):
        await self.market_api.logout()
        async with aiohttp.ClientSession() as session:
            async with session.delete(f"{self._api_root}/interactive/user/session", headers={
                'authorization': self.access_token
            }) as res:
                return res.ok

    async def get_limit_price(self, instrument: Instrument, side: TradeSide):
        ltp = await Ltp.filter(instrument=instrument).get()
        tick_size = 0.05
        if side == TradeSide.BUY:
            limit_price = ltp.price * (1 + 0.5 / 100)
        else:
            limit_price = ltp.price * (1 - 0.5 / 100)
        limit_price = (limit_price // tick_size) * tick_size
        return limit_price
            
    async def place_order(self, trade: Trade) -> dict:
        await trade.fetch_related('instrument')
        inst_id = await self.market_api._get_exchange_instrument_id(trade.instrument)
        limit_price = await self.get_limit_price(trade.instrument, trade.side)
        trade.price = limit_price
        await trade.save()
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self._api_root}/interactive/orders", headers={
                'authorization': self.access_token
            }, json={
                'exchangeSegment': "NSECM" if trade.instrument.stock else "NSEFO",
                'exchangeInstrumentID': int(inst_id),
                'productType': 'NRML',
                'orderType': 'LIMIT',
                'orderSide': trade.side.name,
                'timeInForce': 'DAY',
                'disclosedQuantity': 0,
                'orderQuantity': int(trade.qty),
                'limitPrice': float(limit_price),
                'stopPrice': 0,
                'clientID': '*****'
            }) as res:
                data = await res.json()
                logging.info(f"Place trade response: {data}")
                if not res.ok:
                    raise Exception("SRE place trades failed")
                return data

    async def modify_limit_order_price(self, sre_order: SREOrders):
        await sre_order.fetch_related('trade')
        trade: Trade = sre_order.trade
        await trade.fetch_related('instrument')
        limit_price = await self.get_limit_price(trade.instrument, trade.side)
        trade.price = limit_price
        await trade.save()
        async with aiohttp.ClientSession() as session:
            async with session.put(f"{self._api_root}/interactive/orders", headers={
                'authorization': self.access_token
            }, json={
                'appOrderID': sre_order.app_order_id,
                'modifiedProductType': 'NRML',
                'modifiedOrderType': 'LIMIT',
                'modifiedOrderQuantity': trade.qty,
                'modifiedDisclosedQuantity': 0,
                'modifiedLimitPrice': limit_price,
                'modifiedStopPrice': 0,
                'modifiedTimeInForce': 'DAY',
                'clientID': '*****'
            }) as res:
                return await res.json()

    async def get_order_details(self) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self._api_root}/interactive/orders/dealerorderbook", headers={
                'authorization': self.access_token
            }) as res:
                data = await res.json()
                return data
            
    async def get_portfolio_details(self, day_or_net: Literal["DayWise", "NetWise"] = "DayWise") -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self._api_root}/interactive/portfolio/dealerpositions", headers={
                'authorization': self.access_token
            }, params={
                'dayOrNet': day_or_net
            }) as res:
                data = await res.json()
                return data

    async def place_pending_trades(self):
        sre_orders = await SREOrders.filter(
            sre_account=self.sre_account,
            app_order_id__isnull=True
        ).select_related('trade')
        for sre_order in sre_orders:
            response = await self.place_order(sre_order.trade)
            sre_order.app_order_id = response.get('result', {}).get('AppOrderID')
            await sre_order.save()

    async def check_placed_orders(self) -> List[SREOrders]:
        response = await self.get_order_details()
        orders = response.get('result', [])
        today = datetime.date.today()
        sre_orders = []
        for order in orders:
            sre_order = await SREOrders.filter(app_order_id=order['AppOrderID'], timestamp__gte=today).get_or_none()
            if sre_order:
                if sre_order.status != order['OrderStatus']:
                    sre_order.status = order['OrderStatus']
                    await sre_order.save()
                # if order['OrderStatus'] == 'Open' or order['OrderStatus'] == 'New':
                #     try:
                #         await self.modify_limit_order_price(sre_order)
                #     except Exception as ex:
                #         logging.error(f"Modify order failed for {sre_order.app_order_id}", exc_info=ex)
                sre_orders.append(sre_order)
        return sre_orders


class SRETradeExecutor:

    async def save_trades(self, trades: List[Trade]):
        trades_q = Trade.filter(id__in=[td.id for td in trades])
        all_accounts_ids = trades_q.values_list('subscription__account_id', flat=True)
        sre_accounts = await SREAccount.filter(account_id__in=Subquery(all_accounts_ids)).select_related('account').distinct()
        for sre_account in sre_accounts:
            account_trades = await trades_q.filter(subscription__account=sre_account.account)
            for trade in account_trades:            
                await SREOrders.create(
                    sre_account=sre_account,
                    trade=trade,
                    app_order_id=None
                )

    async def execute_trades(self):
        sre_accounts = await SREAccount.all()
        for sre_account in sre_accounts:
            sre_executor = SREExecute(sre_account)
            await sre_executor.login()
            await sre_executor.place_pending_trades()

    async def check_trades(self):
        sre_accounts = await SREAccount.all()
        sre_orders = []
        for sre_account in sre_accounts:
            sre_executor = SREExecute(sre_account)
            await sre_executor.login()
            sre_orders += await sre_executor.check_placed_orders()
        await SRETradesMailer(sre_orders).run()