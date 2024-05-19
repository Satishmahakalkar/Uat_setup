import datetime
from typing import Dict, List, Literal, Tuple
import settings
import aiohttp
import aioredis
import socketio
import os
import io
import pandas as pd
from database.models import Instrument, Stock
from urllib.parse import urlencode


class SREMarketData:

    def __init__(self) -> None:
        self._access_token = None
        self._user_id = None
        self._api_root = "https://xts.sre.co.in/apimarketdata"
        self._socketio_root = "https://xts.sre.co.in"
        self._instrument_master = pd.DataFrame()
        self.sio = socketio.AsyncClient()
        # self.redis = aioredis.from_url(settings.REDIS_URL)

    @property
    def access_token(self):
        if not self._access_token:
            raise ValueError("Not logged in")
        return self._access_token

    async def login(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self._api_root}/auth/login", json={
                'secretKey': settings.XTS_MARKETDATA_APP_SECRET,
                'appKey': settings.XTS_MARKETDATA_APP_KEY,
                'source': 'WebAPI'
            }) as res:
                if not res.ok:
                    raise ValueError("Login failed")
                data = await res.json()
                self._access_token = data['result']['token']
                self._user_id = data['result']['userID']
            
    async def logout(self):
        async with aiohttp.ClientSession() as session:
            async with session.delete(f"{self._api_root}/auth/logout", headers={
                'authorization': self.access_token
            }) as res:
                return res.ok
            
    def _get_exchange_segment(self, instrument: Instrument):
        if instrument.stock:
            return "NSECM"
        else:
            return "NSEFO"
        
    def _get_base_symbol(self, stock: Stock) -> str:
        ticker = stock.ticker
        if ticker == 'NIFTY 50':
            base_symbol = "NIFTY"
        elif ticker == 'NIFTY BANK':
            base_symbol = "BANKNIFTY"
        else:
            base_symbol = ticker
        return base_symbol
        
    async def get_instrument_master(self):
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self._api_root}/instruments/master", json={
                'exchangeSegmentList': ['NSECM', 'NSEFO']
            }, headers={
                'authorization': self.access_token
            }) as res:
                data = await res.json()
                text: str = data['result']
        df = pd.read_csv(
            io.StringIO(text),
            index_col=False,
            sep='|',
            names=[
                'ExchangeSegment', 'ExchangeInstrumentID', 
                'InstrumentType', 'Name', 'Description', 'Series', 
                'NameWithSeries', 'InstrumentID', 'PriceBand.High', 
                'PriceBand.Low', 'FreezeQty', 'TickSize', 'LotSize', 
                'Multiplier', 'extra1', 'extra2', 
                'extra3', 'extra4', 'extra5', 'extra6', 
                'extra7', 'extra8', 'extra9'
            ]
        )
        mask_eq = df['InstrumentType'] == 8
        mask_fut = df['InstrumentType'] == 1
        mask_opt = df['InstrumentType'] == 2
        df['displayName'] = df[mask_eq]['extra1']
        df['ISIN'] = df[mask_eq]['extra2']
        df['PriceNumerator'] = df[mask_eq]['extra3']
        df['PriceDenominator'] = df[mask_eq]['extra4']
        df['FullName'] = df[mask_eq]['extra5']
        df['UnderlyingInstrumentId'] = df[mask_fut | mask_opt]['extra1']
        df['UnderlyingIndexName'] = df[mask_fut | mask_opt]['extra2']
        df['ContractExpiration'] = df[mask_fut | mask_opt]['extra3']
        df['displayName'].fillna(df[mask_fut]['extra4'], inplace=True)
        df['PriceNumerator'].fillna(df[mask_fut]['extra5'], inplace=True)
        df['PriceDenominator'].fillna(df[mask_fut]['extra6'], inplace=True)
        df['FullName'].fillna(df[mask_fut]['extra7'], inplace=True)
        df['StrikePrice'] = df[mask_opt]['extra4']
        df['OptionType'] = df[mask_opt]['extra5']
        df['displayName'].fillna(df[mask_opt]['extra6'], inplace=True)
        df['PriceNumerator'].fillna(df[mask_opt]['extra7'], inplace=True)
        df['PriceDenominator'].fillna(df[mask_opt]['extra8'], inplace=True)
        df['FullName'].fillna(df[mask_opt]['extra9'], inplace=True)
        df.drop(columns=['extra1', 'extra2', 'extra3', 'extra4', 'extra5', 'extra6', 'extra7', 'extra8', 'extra9'], inplace=True)
        self._instrument_master = df
        return df

    async def _get_exchange_instrument_id(self, instrument: Instrument):
        df = self._instrument_master
        await instrument.fetch_related('stock', 'future__stock', 'option__stock')
        if instrument.stock:
            symbol = self._get_base_symbol(instrument.stock)
            row = df[df['Description'] == f"{symbol}-EQ"].iloc[0]
        elif instrument.future:
            symbol = self._get_base_symbol(instrument.future.stock)
            row = df[df['Description'] == f"{symbol}{instrument.future.expiry.strftime('%y%b').upper()}FUT"].iloc[0]
        elif instrument.option:
            symbol = self._get_base_symbol(instrument.option.stock)
            row = df[df['Description'] == f"{symbol}{instrument.option.expiry.strftime('%y%b').upper()}{instrument.option.strike}{instrument.option.option_type.value}"].iloc[0]
        return row['ExchangeInstrumentID']
    
    async def _market_depth_api(self, instruments: List[dict]) -> Dict[Literal['Bids', 'Asks'], List[Dict[Literal['Price'], float]]]:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self._api_root}/instruments/quotes", json={
                'instruments': instruments,
                'xtsMessageCode': 1502,
                'publishFormat': "JSON"
            }, headers={
                'authorization': self.access_token
            }) as res:
                data = await res.json()
                print(data)
                return data['result']['listQuotes']
    
    async def subscribe_ticker(self, tickers: List[str], segment: Literal["NSECM", "NSEFO"]) -> Dict[Literal['Bids', 'Asks'], List[Dict[Literal['Price'], float]]]:
        df = self._instrument_master
        instruments = [{
            'exchangeInstrumentID': df[df['Description'] == ticker].iloc[0]['ExchangeInstrumentID'],
            'exchangeSegment': segment
        } for ticker in tickers]
        return await self._market_depth_api(instruments)

    async def get_bid_ask(self, instrument: Instrument) -> Tuple[List[Dict[Literal['Price'], float]], List[Dict[Literal['Price'], float]]]:
        market_depth = await self._market_depth_api([{
            'exchangeInstrumentID': int(await self._get_exchange_instrument_id(instrument)),
            'exchangeSegment': "NSECM" if instrument.stock else "NSEFO"
        }])
        return market_depth['Bids'], market_depth['Asks']

    async def _on_connect(self):
        print("Connected")

    async def _on_disconnect(self):
        print("Connected")
    
    async def _on_touchline(self, data: dict):
        print(data)

    async def _on_marketdepth(self, data: dict):
        exchange_instrument_id = data['ExchangeInstrumentID']
        await self.redis.hmset(exchange_instrument_id, data)

    async def _on_candle(self, data: dict):
        print(data)

    async def _on_marketstatus(self, data: dict):
        print(data)

    async def _on_openinterest(self, data: dict):
        print(data)

    async def _on_ltp(self, data: dict):
        print(data)

    async def run_socketio(self):
        self.sio.on('connect', self._on_connect)
        self.sio.on('disconnect', self._on_disconnect)
        self.sio.on('1501-json-full', self._on_touchline)
        self.sio.on('1502-json-full', self._on_marketdepth)
        self.sio.on('1505-json-full', self._on_candle)
        self.sio.on('1507-json-full', self._on_marketstatus)
        self.sio.on('1510-json-full', self._on_openinterest)
        self.sio.on('1512-json-full', self._on_ltp)
        params = urlencode({
            'token': self._access_token,
            'userID': self._user_id,
            'publishFormat': "JSON",
            'broadcastMode': "Full"
        })
        await self.sio.connect(self._socketio_root + "?" + params, socketio_path="/apimarketdata/socket.io", )
        await self.sio.wait()