import logging
from typing import List, Literal
import io
import datetime
import aiohttp
import numpy as np
import settings
import pandas as pd
from asyncio import sleep
from database.models import Future, Instrument, Interval, Ohlc, Ltp, Option, OptionType, Position, Stock, StockGroupMap
from tortoise.models import Q
from tortoise.exceptions import DoesNotExist, MultipleObjectsReturned
from tortoise.expressions import Subquery
from tortoise.transactions import in_transaction


class TrueData:

    def __init__(self) -> None:
        self._access_token = None

    async def login(self) -> str:
        async with aiohttp.ClientSession() as session:
            async with session.post("https://auth.truedata.in/token", data={
                'username': settings.TRUEDATA_USERNAME,
                'password': settings.TRUEDATA_PASSWORD,
                'grant_type': 'password'
            }) as res:
                data = await res.json()
                self._access_token = data['access_token']

    @property
    def access_token(self):
        if not self._access_token:
            raise ValueError("Not logged in")
        return self._access_token
    
    def _get_base_symbol(self, stock: Stock) -> str:
        ticker = stock.ticker
        if ticker == 'NIFTY 50':
            base_symbol = "NIFTY"
        elif ticker == 'NIFTY BANK':
            base_symbol = "BANKNIFTY"
        else:
            base_symbol = ticker
        return base_symbol

    async def get_symbol(self, instrument: Instrument) -> str:
        await instrument.fetch_related('stock', 'future__stock', 'option__stock')
        if instrument.stock:
            symbol = instrument.stock.ticker
        elif instrument.future:
            future: Future = instrument.future
            base_symbol = self._get_base_symbol(future.stock)
            symbol = f"{base_symbol}{future.expiry.strftime('%y%b').upper()}FUT"
        elif instrument.option:
            option: Option = instrument.option
            base_symbol = self._get_base_symbol(option.stock)
            symbol = f"{base_symbol}{option.expiry.strftime('%y%m%d')}{option.strike}{option.option_type.value}"
        else:
            raise ValueError("Cannot get symbol")
        return symbol

    async def get_bhavcopy(self, segment: Literal['EQ', 'FO']):
        async with aiohttp.ClientSession() as session:
            async with session.get("https://history.truedata.in/getbhavcopy", params={
                'segment': segment,
                'date': datetime.date.today().strftime("%Y-%m-%d"),
                'response': 'csv'
            }, headers={
                'Authorization': f"Bearer {self.access_token}"
            }) as res:
                if not res.ok:
                    raise ValueError("Failed")
                data = await res.text()
        df = pd.read_csv(io.StringIO(data))
        if df.empty:
            raise ValueError("Price data fetch failed")
        return df
    
    async def save_historical_data_ltp(self):
        instruments = await Instrument.filter(stock_id__in=Subquery(StockGroupMap.all().values('stock__id')))
        instruments += await Instrument.filter(
            future__stock_id__in=Subquery(StockGroupMap.all().values('stock__id')),
            future__expiry__gte=datetime.date.today()
        )
        ltps = []
        for instrument in instruments:
            symbol = await self.get_symbol(instrument)
            async with aiohttp.ClientSession() as session:
                async with session.get("https://history.truedata.in/getlastnbars", params={
                    'symbol': symbol,
                    'nbars': 1,
                    'response': 'json',
                    'interval': 'eod',
                    'bidask': 0
                }, headers={
                    'Authorization': f"Bearer {self.access_token}"
                }) as res:
                    data = await res.json()
            try:
                price = data['Records'][0][4]
                ltps.append(Ltp(instrument=instrument, price=price))
            except KeyError:
                logging.error(f"Price not found for {symbol}")
            await sleep(0.5)
        instrument_ids = [ltp.instrument.id for ltp in ltps]
        async with in_transaction():
            async with in_transaction():
                await Ltp.filter(instrument_id__in=instrument_ids).delete()
                await Ltp.bulk_create(ltps)

    async def save_historical_data(self, instruments: List[Instrument]):
        async with in_transaction():
            ohlcs = []
            for instrument in instruments:
                await sleep(0.5)
                symbol = await self.get_symbol(instrument)
                logging.info(f"Saving historical data for {symbol}")
                async with aiohttp.ClientSession() as session:
                    async with session.get("https://history.truedata.in/getlastnbars", params={
                        'symbol': symbol,
                        'nbars': 365,
                        'response': 'csv',
                        'interval': 'eod',
                        'bidask': 0
                    }, headers={
                        'Authorization': f"Bearer {self.access_token}"
                    }) as res:
                        data = await res.text()
                        df = pd.read_csv(io.StringIO(data))
                logging.info(f"Entering to db {df.shape[0]}")
                inst_ohlcs = {}
                for row in df.itertuples():
                    ohlc = Ohlc(
                        instrument=instrument,
                        timestamp=row.timestamp,
                        interval=Interval.EOD,
                        open=row.dopen,
                        high=row.dhigh,
                        low=row.dlow,
                        close=row.dclose
                    )
                    inst_ohlcs[row.timestamp] = ohlc
                if inst_ohlcs:
                    timestamp = min(ohlc.timestamp for ohlc in inst_ohlcs.values())
                    await Ohlc.filter(instrument=instrument, interval=Interval.EOD, timestamp__gte=timestamp).delete()
                    ohlcs += list(inst_ohlcs.values())
            await Ohlc.bulk_create(ohlcs)

    async def save_historical_data_for_stocks(self):
        instruments = await Instrument.filter(stock_id__in=Subquery(StockGroupMap.all().values('stock__id')))
        instruments += await Instrument.filter(stock__is_index=True)
        await self.save_historical_data(instruments)

    async def save_historical_data_for_futures(self):
        instruments = await Instrument.filter(
            future__stock_id__in=Subquery(StockGroupMap.all().values('stock__id')),
            future__expiry__gte=datetime.date.today()
        )
        await self.save_historical_data(instruments)

    async def save_ltp_all(self, eq=True, fo=True, ohlc=True):
        if eq:
            df = await self.get_bhavcopy('EQ')
            symbols = df['symbol'].to_list()
            instruments = await Instrument.filter(stock__ticker__in=symbols).select_related('stock')
            now = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            ltps = []
            ohlcs = []
            for instrument in instruments:
                try:
                    row = df[df['symbol'] == instrument.stock.ticker].iloc[0]
                except (IndexError, KeyError):
                    logging.error(f"Error in getting price for {instrument.stock}")
                    continue
                ltps.append(Ltp(instrument=instrument, price=row['close']))
                if ohlc:
                    ohlcs.append(Ohlc(
                        instrument=instrument,
                        timestamp=now,
                        interval=Interval.EOD,
                        open=row['open'],
                        high=row['high'],
                        low=row['low'],
                        close=row['close']
                    ))
            instrument_ids = [inst.id for inst in instruments]
            async with in_transaction():
                await Ltp.filter(instrument_id__in=instrument_ids).delete()
                await Ltp.bulk_create(ltps)
                if ohlc:
                    await Ohlc.filter(instrument_id__in=instrument_ids, interval=Interval.EOD, timestamp__gte=now).delete()
                    await Ohlc.bulk_create(ohlcs)
        if fo:
            df = await self.get_bhavcopy('FO')
            futures = await Instrument.filter(future_id__isnull=False).values('id', 'future__stock__name', 'future__expiry')
            df2 = pd.DataFrame(futures)
            df2['future__expiry'] = pd.to_datetime(df2['future__expiry'])
            df2['symbol'] = df2['future__stock__name'].str.cat(df2['future__expiry'].dt.strftime('%y%b').str.upper())
            df2['symbol'] = df2['symbol'].str.cat(np.repeat('FUT', df2.shape[0]))
            df2 = pd.merge(df, df2, on='symbol')
            ltps = []
            for row in df2.itertuples():
                ltps.append(Ltp(instrument_id=row.id, price=row.close))
            future_ids = df2['id'].to_list()
            options = await Instrument.filter(option_id__isnull=False).values('id', 'option__stock__name', 'option__strike', 'option__expiry', 'option__option_type')
            df2 = pd.DataFrame(options)
            df2['option__expiry'] = pd.to_datetime(df2['option__expiry'])
            df2['symbol'] = df2['option__stock__name'].str.cat(df2['option__expiry'].dt.strftime('%y%m%d'))
            df2['symbol'] = df2['symbol'].str.cat(df2['option__strike'].astype('str')).str.cat(
                df2['option__option_type'].apply(lambda x: getattr(x, 'value'))
            )
            df2 = pd.merge(df, df2, on='symbol')
            for row in df2.itertuples():
                ltps.append(Ltp(instrument_id=row.id, price=row.close))
            option_ids = df2['id'].to_list()
            instrument_ids = future_ids + option_ids
            async with in_transaction():
                await Ltp.filter(instrument_id__in=instrument_ids).delete()
                await Ltp.bulk_create(ltps)

    async def populate_instruments(self):
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.truedata.in/getAllSymbols", params={
                'segment': 'eq',
                'user': settings.TRUEDATA_USERNAME,
                'password': settings.TRUEDATA_PASSWORD,
                'csv': 'true',
                'allexpiry': 'false'
            }) as res:
                data = await res.text()
                df = pd.read_csv(io.StringIO(data),
                    names=['truedata_id', 'symbol', 'type', 'isin', 'exchange', 'lot_size', 'strike', 'expiry', 'extra_1', 'extra_2']    
                )
        for row in df.itertuples():
            if row.type in ['EQ', 'IN']:
                stock = await Stock.filter(ticker=row.symbol).get_or_none()
                is_index = row.type == 'IN'
                if not stock:
                    stock = await Stock.create(
                        ticker=row.symbol,
                        isin=row.isin,
                        is_index=is_index,
                        name=row.extra_1,
                    )
                elif stock.is_index != is_index or stock.name != row.extra_1 or stock.ticker != row.symbol:
                    stock.ticker = row.symbol
                    stock.is_index = is_index
                    stock.name = row.extra_1
                    await stock.save()
                await Instrument.get_or_create(
                    stock=stock,
                    future=None,
                    option=None
                )
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.truedata.in/getAllSymbols", params={
                'segment': 'fo',
                'user': settings.TRUEDATA_USERNAME,
                'password': settings.TRUEDATA_PASSWORD,
                'csv': 'true',
                'allexpiry': 'false'
            }) as res:
                data = await res.text()
                df = pd.read_csv(io.StringIO(data),
                    names=['truedata_id', 'symbol', 'type', 'isin', 'exchange', 'lot_size', 'strike', 'expiry', 'extra_1', 'extra_2']    
                )
        df['name'] = df['symbol'].str.extract(r"([A-Z&\-]+)\d{2}[A-Z]{3}FUT")[0]
        opt_name = df['symbol'].str.extract(r"([A-Z&\-]+)\d{2}\d{2}\d+(C|P)E")[0]
        df['name'] = df['name'].fillna(opt_name)
        df = df.dropna(subset='name').reset_index(drop=True)
        df2 = await self.get_bhavcopy(segment='FO')
        df = pd.merge(df, df2, on='symbol').reset_index(drop=True)
        for row in df.itertuples():
            try:
                stock = await Stock.filter(name=row.name).get()
            except (DoesNotExist, MultipleObjectsReturned):
                logging.error(f"Error for stock {row}")
                continue
            if row.type == 'XX':
                fut, _ = await Future.get_or_create(
                    stock=stock,
                    expiry=datetime.datetime.strptime(row.expiry, "%d-%m-%Y").date(),
                    defaults=dict(
                        lot_size=row.lot_size
                    )
                )
                if fut.lot_size != row.lot_size:
                    fut.lot_size = row.lot_size
                    await fut.save()
                await Instrument.get_or_create(
                    stock=None,
                    future=fut,
                    option=None
                )
            else:
                opt, _ = await Option.get_or_create(
                    stock=stock,
                    strike=row.strike,
                    expiry=datetime.datetime.strptime(row.expiry, "%d-%m-%Y").date(),
                    option_type=OptionType(row.type),
                    defaults=dict(
                        lot_size=row.lot_size,
                    )
                )
                if opt.lot_size != row.lot_size:
                    opt.lot_size = row.lot_size
                    await opt.save()
                await Instrument.get_or_create(
                    stock=None,
                    future=None,
                    option=opt
                )
