import datetime
from typing import List
import numpy as np
import pandas as pd
import pytz
from accounts.pnl import PnlSave
from algos.componentanalysis import ShadowPositionCompAnalysis
from database.models import Account, Instrument, Ltp, Subscription, SubscriptionData, TradeSide
from tortoise.expressions import Subquery
import settings
from aiogoogle import Aiogoogle
from aiogoogle.auth.creds import ServiceAccountCreds
from aiogoogle.models import Request


class GoogleSheetEdit:

    def __init__(self) -> None:
        self.aiogoogle = Aiogoogle(
            service_account_creds=ServiceAccountCreds(
                scopes=["https://www.googleapis.com/auth/spreadsheets"],
                **settings.GOOGLE_SERVICE_ACCOUNT_CREDS
            )
        )
        self.service = None
        self.spreadsheet_id = settings.SHADOW_ANALYSIS_SHEET_ID
        self.futures_price_sheet = "BotEditFuturesPrices"
        self.mtm_sheet = "MTM Comparison"
        self.sheet_names = []
        self._request_split_threshold = 5

    async def init(self):
        async with self.aiogoogle:
            self.service = await self.aiogoogle.discover("sheets", "v4")
            req = self.service.spreadsheets.get(spreadsheetId=self.spreadsheet_id)
            res = await self.aiogoogle.as_service_account(req)
            self.sheet_names = [sheet['properties']['title'] for sheet in res['sheets']]

    async def execute_requests(self, reqs):
        if isinstance(reqs, list):
            for _reqs in np.array_split(reqs, self._request_split_threshold):
                async with self.aiogoogle:
                    res = await self.aiogoogle.as_service_account(*list(_reqs))
        else:
            async with self.aiogoogle:
                res = await self.aiogoogle.as_service_account(reqs)
        return res

    def get_request(self, spread_sheet_name: str, range: str):
        return self.service.spreadsheets.values.get(
            spreadsheetId=self.spreadsheet_id,
            range=f"{spread_sheet_name}!{range}"
        )
    
    def update_request(self, spread_sheet_name: str, range: str, values: list):
        return self.service.spreadsheets.values.update(
            spreadsheetId=self.spreadsheet_id,
            range=f"{spread_sheet_name}!{range}",
            valueInputOption="USER_ENTERED",
            json=dict(
                values=values
            )
        )
    
    def append_request(self, spread_sheet_name: str, range: str, values: list):
        return self.service.spreadsheets.values.append(
            spreadsheetId=self.spreadsheet_id,
            range=f"{spread_sheet_name}!{range}",
            valueInputOption="USER_ENTERED",
            json=dict(
                values=values
            )
        )
    
    def clear_request(self, spread_sheet_name: str, range: str):
        return self.service.spreadsheets.values.clear(
            spreadsheetId=self.spreadsheet_id,
            range=f"{spread_sheet_name}!{range}"
        )
    
    def add_sheet_request(self, sheet_name: str):
        return self.service.spreadsheets.batchUpdate(spreadsheetId=self.spreadsheet_id, json={
            "requests": [{
                "addSheet": {"properties": {"title": sheet_name}}
            }]
        })

    async def get_data(self, spread_sheet_name: str, range: str):
        req = self.get_request(spread_sheet_name, range)
        res = await self.execute_requests(req)
        return res.get('values', [])
    
    async def update_data(self, spread_sheet_name: str, range: str, values: list):
        req = self.update_request(spread_sheet_name, range, values)
        await self.execute_requests(req)

    async def append_data(self, spread_sheet_name: str, values: list):
        req = self.append_request(spread_sheet_name, "A:Z", values)
        await self.execute_requests(req)

    async def update_futures_prices(self):
        values = await self.get_data(self.futures_price_sheet, "A:A")
        values_updated = []
        today = datetime.date.today()
        for row in values:
            if row[0] == 'Ticker':
                values_updated.append([row[0], "CurrentFuturesPrice"])
            else:
                instrument = await Instrument.filter(future__stock__ticker=row[0], future__expiry__gt=today).order_by('future__expiry').first()
                ltp = await Ltp.filter(instrument=instrument).get()
                values_updated.append([row[0], ltp.price])
        await self.update_data(self.futures_price_sheet, "A:B", values_updated)

    async def update_shadow_positions(self):
        excel = await PnlSave.generate_shadow_positions_excel()
        xl = pd.ExcelFile(excel)
        reqs_batch_1 = []
        reqs_batch_2 = []
        for sheet_name in xl.sheet_names:
            df = pd.read_excel(xl, sheet_name=sheet_name)
            df.fillna("", inplace=True)
            if 'old_price' in df.columns and 'exit_time' in df.columns:
                df = df[['ticker', 'side', 'qty' , 'entry_time', 'price', 'old_price', 'exit_price', 'exit_time', 'mtm']]
            elif 'old_price' in df.columns:
                df = df[['ticker', 'side', 'qty' , 'entry_time', 'price', 'old_price', 'mtm']]
            else:
                df = df[['ticker', 'side', 'qty' , 'entry_time', 'price']]
            if sheet_name not in self.sheet_names:
                req = self.add_sheet_request(sheet_name)
            else:
                req = self.clear_request(sheet_name, "A:I")
            reqs_batch_1.append(req)
            values = [df.columns.tolist()] + df.values.tolist()
            req = self.update_request(sheet_name, "A:I", values)
            reqs_batch_2.append(req)
        await self.execute_requests(reqs_batch_1)
        await self.execute_requests(reqs_batch_2)

    async def append_shadow_mtms(self):
        sub_datas = await SubscriptionData.filter(
            subscription__active=True, subscription__is_hedge=False
        ).select_related('subscription__account')
        mtms = {}
        for sub_data in sub_datas:
            try:
                long_mtm = sub_data.data['long_mtm_tracking'][-1]
                short_mtm = sub_data.data['short_mtm_tracking'][-1]
            except (KeyError, IndexError):
                continue
            mtms[sub_data.subscription.account.name] = (long_mtm, short_mtm)
        header_range = "A1:ZZ1"
        headers = await self.get_data(self.mtm_sheet, header_range)
        if headers:
            headers = headers[0]
        else:
            headers = ["Timestamp"]
        row: list = np.zeros_like(headers).tolist()
        header_changed = False
        for account_name, (long_mtm, short_mtm) in mtms.items():
            long_name = f"{account_name} LONG"
            short_name = f"{account_name} SHORT"
            try:
                idx = headers.index(long_name)
                row[idx] = long_mtm
            except ValueError:
                headers.append(long_name)
                row.append(long_mtm)
                header_changed = True
            try:
                idx = headers.index(short_name)
                row[idx] = short_mtm
            except ValueError:
                headers.append(short_name)
                row.append(short_mtm)
                header_changed = True
        row[0] = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).replace(tzinfo=None).isoformat()
        if header_changed:
            await self.update_data(self.mtm_sheet, header_range, [headers])
        await self.append_data(self.mtm_sheet, [row])

    async def restore_shadow_positions(self):
        accounts = await Account.filter(
            id__in=Subquery(Subscription.filter(active=True).values('account_id'))
        )
        today = datetime.date.today()
        for account in accounts:
            if account.name in self.sheet_names:
                data = await self.get_data(account.name, "A:Z")
                sub_data = await SubscriptionData.filter(subscription__account=account, subscription__is_hedge=False).get()
                stored_positions = []
                df = pd.DataFrame(data=data[1:], columns=data[0])
                for row in df.itertuples():
                    instrument = await Instrument.filter(
                        future__stock__ticker=row.ticker,
                        future__expiry__gt=today
                    ).order_by('future__expiry').first()
                    if not instrument:
                        continue
                    values = {
                        'inst_id': instrument.id,
                        'price': float(row.price),
                        'side': row.side,
                        'qty': int(row.qty),
                        'entry_time': datetime.datetime.strptime(row.entry_time, "%Y-%m-%d %H:%M:%S").isoformat()
                    }
                    if row.old_price:
                        values['old_price'] = float(row.old_price)
                    if row.exit_price:
                        values['exit_price'] = float(row.exit_price)
                    if row.exit_time:
                        values['exit_time'] = datetime.datetime.strptime(row.exit_time, "%Y-%m-%d %H:%M:%S").isoformat()
                    if row.mtm:
                        values['mtm'] = float(row.mtm)
                    stored_positions.append(values)
                sub_data.data['positions'] = stored_positions
                await sub_data.save()

    async def update_trade_counter_ratios(self):
        subs_q = Subscription.filter(active=True, is_hedge=False).values('id')
        sub_datas = await SubscriptionData.filter(
            subscription_id__in=Subquery(subs_q)
        ).select_related('subscription__account')
        values = [["Account", "Ratio"]]
        for sub_data in sub_datas:
            trade_counter_ratio = sub_data.data.get('trade_counter_ratio')
            if trade_counter_ratio:
                values.append([sub_data.subscription.account.name, trade_counter_ratio])
        await self.update_data("TradeCounterRatio", "A:B", values)

    async def component_analysis(self):
        subs_q = Subscription.filter(active=True, algo__name="ComponentAnalysis")
        sub_datas = await SubscriptionData.filter(
            subscription__id__in=Subquery(subs_q.values('id'))
        ).select_related('subscription__account')
        now = datetime.datetime.now()
        today = now.date()
        now_time = now.time().replace(second=0, microsecond=0)
        requests = []
        columns = ['stock_name', 'side', 'qty' , 'entry_time', 'price', 'old_price', 'mtm', 'min_move_stock', 'action', 'time']
        for sub_data in sub_datas:
            sheet_name = f"{sub_data.subscription.account.name}_ComponentAnalysis"
            if sheet_name not in self.sheet_names:
                req = self.add_sheet_request(sheet_name)
                requests.append(req)
                req = self.append_request(sheet_name, "A:B", [["Component Analysis Date", today.isoformat()]])
                requests.append(req)
            else:
                analysis_date_str = await self.get_data(sheet_name, "B1")
                analysis_date = pd.to_datetime(analysis_date_str[0][0]).date()
                if analysis_date < today:
                    req = self.clear_request(sheet_name, "A:I")
                    requests.append(req)
                    req = self.append_request(sheet_name, "A:B", [["Component Analysis Date", today.isoformat()]])
                    requests.append(req)
            shadow_positions: List[ShadowPositionCompAnalysis] = sub_data.data.get('positions', [])
            df = pd.DataFrame(shadow_positions)
            df['min_move_stock'] = sub_data.data.get('min_move_stock', 0)
            df['time'] = now_time.isoformat()
            df['inst_id'] = df['inst_id'].astype('int')
            instruments = await Instrument.filter(id__in=df['inst_id'].to_list()).select_related('future__stock')
            df['stock_name'] = df['inst_id'].replace({inst.id: inst.future.stock.name for inst in instruments})
            df = df[columns]
            rows: list = df.values.tolist()
            rows.insert(0, columns)
            rows.append(np.full_like(columns, '-').tolist())
            rows.append(np.full_like(columns, '-').tolist())
            req = self.append_request(sheet_name, "A:Z", rows)
            requests.append(req)
        await self.execute_requests(requests)