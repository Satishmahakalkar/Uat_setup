import datetime
import logging
import aiohttp
import pandas as pd
from algos.basealgo import BaseAlgo
from database.models import Account, Algo, Instrument, Ltp, Option, OptionType, Position, Stock, Subscription, TradeSide
from tortoise.expressions import F, Subquery
import settings


class ResultsHedgeAlgo(BaseAlgo):

    async def init(self):
        self.algo = await Algo.get(name=self.__class__.__name__)

    @staticmethod
    async def get_results_stock_names() -> list:
        today = pd.Timestamp.today().date()
        next_day = (pd.offsets.BusinessDay(n=1) + today).date()
        while next_day in settings.HOLIDAY_DATES:
            next_day = pd.offsets.BusinessDay(n=1) + next_day
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.bseindia.com/BseIndiaAPI/api/Corpforthresults/w", headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.5',
                'Origin': 'https://www.bseindia.com',
                'Connection': 'keep-alive',
                'Referer': 'https://www.bseindia.com/',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-site',
                'Pragma': 'no-cache',
                'Cache-Control': 'no-cache'
            }) as res:
                data = await res.json()
        df = pd.DataFrame(data)
        df = df[['short_name', 'meeting_date']]
        df['meeting_date'] = pd.to_datetime(df['meeting_date'])
        df = df[(df['meeting_date'].dt.date == next_day) | (df['meeting_date'].dt.date == today)]
        return df['short_name'].to_list()

    async def get_option(self, stock: Stock, side: TradeSide) -> Option:
        option = await Option.filter(stock=stock, expiry__gt=datetime.date.today()).order_by('expiry').first()
        expiry = option.expiry
        option_type = OptionType.CALL if side == TradeSide.SELL else OptionType.PUT
        ltp = await Ltp.filter(instrument__stock=stock).get()
        values = await Option.filter(stock=stock, expiry=expiry, option_type=option_type).annotate(
            diff_strike = F('strike') - ltp.price
        ).values('diff_strike', 'strike')
        strike = min(values, key = lambda value: abs(value['diff_strike']))['strike']
        opt = await Option.filter(stock=stock, expiry=expiry, strike=strike, option_type=option_type).get()
        return opt

    async def run(self):
        tickers = await self.get_results_stock_names()
        account_ids_q = Subscription.filter(algo=self.algo, active=True).values_list('account_id')
        accounts = await Account.filter(id__in=Subquery(account_ids_q))
        futures_positions = Position.filter(
            active=True,
            instrument__future__stock__ticker__in=tickers,
            subscription__is_hedge=False,
            subscription__account_id__in=Subquery(account_ids_q)
        ).select_related('instrument__future__stock', 'subscription__account')
        hedge_positions = Position.filter(
            active=True,
            subscription__account_id__in=Subquery(account_ids_q),
            subscription__algo=self.algo,
            subscription__is_hedge=True
        ).select_related('instrument__option__stock')
        to_exit = await hedge_positions.exclude(instrument__option__stock__ticker__in=tickers)
        to_exit2 = await hedge_positions.exclude(instrument_id__in=Subquery(futures_positions.values('instrument_id')))
        to_exit = set(to_exit).union(to_exit2)
        for position in to_exit:
            ltp = await Ltp.filter(instrument=position.instrument).get()
            await self.exit(position, ltp.price)
        for account in accounts:
            positions = await futures_positions.filter(subscription__account=account)
            for position in positions:
                if not await hedge_positions.filter(
                    subscription__account=account,
                    instrument__option__stock=position.instrument.future.stock
                ).exists():
                    sub = await Subscription.filter(account=account, active=True, algo=self.algo).get()
                    opt = await self.get_option(position.instrument.future.stock, position.side)
                    instrument = await Instrument.filter(option=opt).get()
                    lots = position.qty / position.instrument.future.lot_size
                    option_qty = lots * opt.lot_size
                    try:
                        ltp = await Ltp.filter(instrument=instrument).get()
                        price = ltp.price
                    except:
                        price = 0
                    await self.entry(sub, instrument, option_qty, TradeSide.BUY, price)
