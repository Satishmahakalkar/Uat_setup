

import datetime
import importlib
from io import StringIO
import logging
from typing import Literal
import aiohttp
import pandas as pd
from algos.basealgo import BaseAlgo
from database.models import Account, AccountEmail, Algo, Future, Instrument, Investment, Position, SREAccount, Stock, StockGroup, StockGroupMap, Strategy, Subscription, SubscriptionData, Trade, TradeSide, User


class Seed:

    async def save_nifty_stock_group(self):
        sg, _ = await StockGroup.get_or_create(name='Nifty50')
        # niftyindices.com blocks scraping
        stock_names = ["ADANIENT","ADANIPORTS","APOLLOHOSP","ASIANPAINT","AXISBANK","BAJAJ-AUTO","BAJFINANCE","BAJAJFINSV","BPCL","BHARTIARTL","BRITANNIA","CIPLA","COALINDIA","DIVISLAB","DRREDDY","EICHERMOT","GRASIM","HCLTECH","HDFCBANK","HDFCLIFE","HEROMOTOCO","HINDALCO","HINDUNILVR","ICICIBANK","ITC","INDUSINDBK","INFY","JSWSTEEL","JIOFIN","KOTAKBANK","LTIM","LT","M&M","MARUTI","NTPC","NESTLEIND","ONGC","POWERGRID","RELIANCE","SBILIFE","SBIN","SUNPHARMA","TCS","TATACONSUM","TATAMOTORS","TATASTEEL","TECHM","TITAN","UPL","ULTRACEMCO","WIPRO"]
        for ticker in stock_names:
            stock = await Stock.get_or_none(ticker=ticker)
            if stock:
                await StockGroupMap.create(stock_group=sg, stock=stock)
            else:
                logging.error(f"Stock not available {ticker}")

    async def save_nifty_next_50_stock_group(self):
        sg, _ = await StockGroup.get_or_create(name='NiftyNext50')
        # niftyindices.com blocks scraping
        stock_names = ["ABB", "ACC", "ADANIENSOL", "ADANIGREEN", "ATGL", "AWL", "AMBUJACEM", "DMART", "BAJAJHLDNG", "BANKBARODA", "BERGEPAINT", "BEL", "BOSCHLTD", "CANBK", "CHOLAFIN", "COLPAL", "DLF", "DABUR", "NYKAA", "GAIL", "GODREJCP", "HDFCAMC", "HAVELLS", "HAL", "ICICIGI", "ICICIPRULI", "IOC", "IRCTC", "INDUSTOWER", "NAUKRI", "INDIGO", "JINDALSTEL", "LICI", "MARICO", "MUTHOOTFIN", "PIIND", "PAGEIND", "PIDILITIND", "PGHH", "SBICARD", "SRF", "MOTHERSON", "SHREECEM", "SIEMENS", "TATAPOWER", "TORNTPHARM", "MCDOWELL-N", "VBL", "VEDL", "ZOMATO"]
        today = datetime.date.today()
        for ticker in stock_names:
            stock = await Stock.get_or_none(ticker=ticker)
            if stock:
                fut_exists = await Future.filter(stock=stock, expiry__gte=today).exists()
                if fut_exists:
                    await StockGroupMap.create(stock_group=sg, stock=stock)
                else:
                    logging.error(f"Fut not available {ticker}")
            else:
                logging.error(f"Stock not available {ticker}")

    async def save_balance_futures(self):
        sg, _ = await StockGroup.get_or_create(name='NiftyNextBalance')
        stock_names = ["AARTIIND", "ABB", "ABBOTINDIA", "ABCAPITAL", "ABFRL", "ADANIENT", "ACC", "ADANIPORTS", "ALKEM", "AMBUJACEM", "APOLLOHOSP", "APOLLOTYRE", "ASHOKLEY", "ASIANPAINT", "ASTRAL", "ATUL", "AUBANK", "AUROPHARMA", "BSOFT", "AXISBANK", "BAJAJ-AUTO", "BEL", "BATAINDIA", "BHARATFORG", "BAJAJFINSV", "BAJFINANCE", "BHEL", "CANFINHOME", "CIPLA", "COLPAL", "CUMMINSIND", "DRREDDY", "ESCORTS", "GAIL", "GMRINFRA", "COROMANDEL", "GODREJPROP", "DIXON", "HAL", "HCLTECH", "GNFC", "HDFCAMC", "HINDALCO", "HINDCOPPER", "IDEA", "IEX", "INDHOTEL", "INDIACEM", "IBULHSGFIN", "BALKRISIND", "BALRAMCHIN", "BANDHANBNK", "IPCALAB", "INDUSTOWER", "IRCTC", "JINDALSTEL", "LICHSGFIN", "M&MFIN", "LTIM", "MARICO", "LTTS", "MCX", "M&M", "MARUTI", "NTPC", "MFSL", "OBEROIRLTY", "NATIONALUM", "NMDC", "PFC", "POLYCAB", "RECLTD", "BANKBARODA", "BERGEPAINT", "DABUR", "SUNPHARMA", "SUNTV", "OFSS", "ONGC", "DALBHARAT", "SYNGENE", "GLENMARK", "HINDUNILVR", "ICICIGI", "TATACHEM", "PERSISTENT", "INDIAMART", "JUBLFOOD", "KOTAKBANK", "PNB", "TATACOMM", "POWERGRID", "LT", "SAIL", "TATAPOWER", "TATASTEEL", "TECHM", "UBL", "ZEEL", "TORNTPHARM", "TVSMOTOR", "BHARTIARTL", "BRITANNIA", "BIOCON", "BOSCHLTD", "CANBK", "CHOLAFIN", "COALINDIA", "COFORGE", "CROMPTON", "DLF", "CUB", "EICHERMOT", "GUJGASLTD", "EXIDEIND", "FEDERALBNK", "HEROMOTOCO", "GODREJCP", "GRANULES", "HAVELLS", "HDFCBANK", "IDFC", "IDFCFIRSTB", "HINDPETRO", "ICICIBANK", "JSWSTEEL", "LUPIN", "INDUSINDBK", "INFY", "METROPOLIS", "MGL", "JKCEMENT", "MRF", "MUTHOOTFIN", "L&TFH", "LALPATHLAB", "DEEPAKNTR", "CONCOR", "MANAPPURAM", "ICICIPRULI", "PAGEIND", "IGL", "NAUKRI", "PETRONET", "NESTLEIND", "PEL", "BPCL", "RAMCOCEM", "RBLBANK", "RELIANCE", "PVRINOX", "SBICARD", "SBILIFE", "SBIN", "SHRIRAMFIN", "SRF", "TATACONSUM", "TATAMOTORS", "MOTHERSON", "SIEMENS", "MPHASIS", "TCS", "TITAN", "ULTRACEMCO", "UPL", "VEDL", "WIPRO", "ZYDUSLIFE", "CHAMBLFERT", "DELTACORP", "DIVISLAB", "GRASIM", "HDFCLIFE", "INDIGO", "IOC", "ITC", "LAURUSLABS", "SHREECEM", "MCDOWELL-N", "NAVINFLUOR", "PIDILITIND", "PIIND", "VOLTAS", "TRENT"]
        nifty_stocks = await StockGroupMap.filter(stock_group__name='Nifty50').values_list('stock__ticker', flat=True)
        next_50_stocks = await StockGroupMap.filter(stock_group__name='NiftyNext50').values_list('stock__ticker', flat=True)
        stock_names = set(stock_names)
        nifty_stocks = set(nifty_stocks)
        next_50_stocks = set(next_50_stocks)
        stock_names = stock_names - (nifty_stocks | next_50_stocks)
        today = datetime.date.today()
        for ticker in stock_names:
            stock = await Stock.get_or_none(ticker=ticker)
            if stock:
                fut_exists = await Future.filter(stock=stock, expiry__gte=today).exists()
                if fut_exists:
                    await StockGroupMap.create(stock_group=sg, stock=stock)
                else:
                    logging.error(f"Fut not available {ticker}")
            else:
                logging.error(f"Stock not available {ticker}")

    async def add_algo(self, algo_name: str):
        algo, _ = await Algo.get_or_create(name=algo_name)
        return algo

    async def add_strategy(self, strategy_name: str):
        strategy, _ = await Strategy.get_or_create(name=strategy_name)
        return strategy
    
    async def add_account(self, email: str, account_name: str):
        user, _ = await User.get_or_create(email=email)
        account = await Account.create(user=user, start_date=datetime.date.today(), name=account_name)
        return account

    async def add_investment(self, account_id: int, amount: float):
        account = await Account.get(id=account_id)
        return await Investment.create(account=account, amount=amount)

    async def add_subscription(self, account_id: int, algo_id: int, is_hedge=False):
        algo = await Algo.get(id=algo_id)
        account = await Account.get(id=account_id)
        sub, _ = await Subscription.get_or_create(account=account, algo=algo, defaults=dict(start_date=datetime.date.today(), is_hedge=is_hedge))
        return sub
    
    async def add_cc_email(self, account_id: int, emails: list):
        account = await Account.get(id=account_id)
        for email in emails:
            await AccountEmail.get_or_create(account=account, email=email)
    
    async def change_subscription(self, account: Account, algo: Algo, dry_run=False):
        sub = await Subscription.filter(account=account, is_hedge=False).select_related('algo').get()
        print(f"Changing from {sub.algo.name} to {algo.name} for {account.name}")
        sub.algo = algo
        if not dry_run:
            await sub.save()
            await SubscriptionData.filter(subscription=sub).delete()
        return sub
    
    async def add_position(self, subscription_id: int, side: Literal["buy", "sell"], ticker: str, qty: int, bought_price: float):
        today = datetime.date.today()
        sub = await Subscription.get(id=subscription_id).select_related('algo')
        instrument = await Instrument.filter(future__stock__ticker=ticker, future__expiry__gte=today).order_by('future__expiry').first()
        module = importlib.import_module(f'algos.{sub.algo.name.lower()}')
        algo_strat_class = getattr(module, sub.algo.name)
        algo: BaseAlgo = algo_strat_class()
        await algo.entry(sub, instrument, qty, TradeSide(side), bought_price)