import datetime
from decimal import Decimal
from io import BytesIO
from itertools import repeat
import logging
import numpy as np
from xlsxwriter import Workbook, worksheet
import pandas as pd
from algos.basealgo import BaseAlgo
from database.models import Account, Instrument, Investment, Ltp, PnL, Position, Subscription, SubscriptionData, TradeExit, TradeSide
from tortoise.functions import Sum
from tortoise.expressions import F, Subquery, Q
from pypika.functions import Extract


class PnlSave:

    async def save_eod_price(self):
        positions = await Position.filter(active=True).select_related('instrument')
        for position in positions:
            ltp = await Ltp.filter(instrument=position.instrument).get()
            position.eod_price = Decimal(ltp.price)
            if position.buy_price:
                position.charges = (
                    BaseAlgo.charges_calculate(position.qty, position.buy_price, TradeSide.BUY)
                    + BaseAlgo.charges_calculate(position.qty, position.eod_price, TradeSide.SELL)
                )
            else:
                position.charges = (
                    BaseAlgo.charges_calculate(position.qty, position.sell_price, TradeSide.SELL)
                    + BaseAlgo.charges_calculate(position.qty, position.eod_price, TradeSide.BUY)
                )
            if position.side == TradeSide.BUY:
                position.pnl = (position.eod_price - position.buy_price) * position.qty
            else:
                position.pnl = (position.sell_price - position.eod_price) * position.qty
            await position.save()

    async def save_pnl(self, account: Account):
        investment = (await Investment.filter(
            account=account
        ).annotate(sum=Sum('amount')).first().values('sum'))['sum']
        subs_q = Subscription.filter(account=account).values('id')
        positions = Position.filter(subscription_id__in=Subquery(subs_q))
        realised_pnl = await positions.filter(active=False).annotate(sum=Sum('pnl')).first().values_list('sum', flat=True) or 0
        unrealised_pnl = await positions.filter(active=True).annotate(sum=Sum('pnl')).first().values_list('sum', flat=True) or 0
        await PnL.update_or_create(
            account=account,
            date=datetime.date.today(),
            defaults=dict(
                investment=investment,
                unrealised_pnl=unrealised_pnl,
                realised_pnl=realised_pnl
            )
        )

    @staticmethod
    async def generate_pnl_excel(account: Account):
        trade_exits = TradeExit.filter(position__subscription__account=account)
        opens_data = await trade_exits.filter(position__active=True).values(
            future_stock_name = 'position__instrument__future__stock__ticker',
            option_stock_name = 'position__instrument__option__stock__ticker',
            strike = 'position__instrument__option__strike',
            option_expiry = 'position__instrument__option__expiry',
            future_expiry = 'position__instrument__future__expiry',
            qty = 'position__qty',
            buy_price = 'position__buy_price',
            sell_price = 'position__sell_price',
            side = 'position__side',
            charges = 'position__charges',
            cmp = 'position__eod_price',
            mtm = 'position__pnl',
            entry_time = 'entry_trade__timestamp',
        )
        closed_data = await trade_exits.filter(position__active=False).values(
            future_stock_name = 'position__instrument__future__stock__ticker',
            option_stock_name = 'position__instrument__option__stock__ticker',
            strike = 'position__instrument__option__strike',
            option_expiry = 'position__instrument__option__expiry',
            future_expiry = 'position__instrument__future__expiry',
            qty = 'position__qty',
            buy_price = 'position__buy_price',
            sell_price = 'position__sell_price',
            side = 'position__side',
            charges = 'position__charges',
            pnl = 'position__pnl',
            entry_time = 'entry_trade__timestamp',
            exit_time = 'exit_trade__timestamp',
        )

        def create_df(data):
            df = pd.DataFrame(data)
            if df.empty:
                return df
            df['entry_time'] = df['entry_time'].dt.tz_convert('Asia/Kolkata').dt.tz_localize(None).dt.round(freq='s')
            if 'exit_time' in df.columns:
                df['exit_time'] = df['exit_time'].dt.tz_convert('Asia/Kolkata').dt.tz_localize(None).dt.round(freq='s')
            df['side'] = df['side'].apply(lambda side: "LONG" if side == TradeSide.BUY else "SHORT")
            df['stock_name'] = df['future_stock_name'].fillna(df['option_stock_name'])
            df['expiry'] = df['future_expiry'].fillna(df['option_expiry'])
            df['expiry'] = pd.to_datetime(df['expiry'])
            if 'exit_time' in df.columns:
                df = df[['stock_name', 'strike', 'expiry', 'qty', 'buy_price', 'sell_price', 'side', 'charges', 'pnl', 'entry_time', 'exit_time']]
            else:
                df = df[['stock_name', 'strike', 'expiry', 'qty', 'buy_price', 'sell_price', 'side', 'charges', 'cmp', 'mtm', 'entry_time']]
            return df

        df_open = create_df(opens_data)
        df_closed = create_df(closed_data)
        try:
            expiry_mask = df_closed['expiry'].dropna().dt.year.round().astype('str') + '-' + df_closed['expiry'].dt.strftime("%b")
        except KeyError:
            expiry_mask = pd.Series()
        sheet_names = expiry_mask.unique()
        sheet_names = sorted(sheet_names, key= lambda month_year: datetime.datetime.strptime(month_year[-3:], "%b").month, reverse=True)
        sheet_names.insert(0, "Summary")
        if not df_open.empty:
            sheet_names.insert(1, "OpenPositions")
        fp = BytesIO()
        try:
            with pd.ExcelWriter(fp, engine='xlsxwriter', engine_kwargs={'options': {'strings_to_numbers': True}}) as excel:
                summary = {}
                last_row_idxs = {}
                for sheet_name in sheet_names:
                    excel.book.add_worksheet(sheet_name)
                if not df_open.empty:
                    last_row = ["TOTAL", '-', '-', '-', '-', '-', '-', df_open['charges'].sum(), '-', df_open['mtm'].sum(), '-']
                    df_open.loc[df_open.shape[0]] = last_row
                    sheet_name = "OpenPositions"
                    df_open.to_excel(excel, sheet_name=sheet_name, index=False)
                    last_row_idxs[sheet_name] = df_open.shape[0]
                    sheet_names = sheet_names[2:]
                else:
                    sheet_names = sheet_names[1:]
                for expiry in sheet_names:
                    df: pd.DataFrame = df_closed[expiry == expiry_mask].reset_index(drop=True)
                    charges = df['charges'].sum()
                    pnl = df['pnl'].sum()
                    last_row = ["TOTAL", '-', '-', '-', '-', '-', '-', charges, pnl, '-', '-']
                    df.loc[df.shape[0]] = last_row
                    last_row_idxs[expiry] = df.shape[0]
                    df.to_excel(excel, sheet_name=expiry, index=False)
                    summary[expiry] = {'cost': charges, 'pnl': pnl}
                wb: Workbook = excel.book
                num_format = wb.add_format({'num_format': "#,##0.00"})
                bold_format = wb.add_format()
                bold_format.set_bold()
                sheets = wb.sheetnames
                for sheet_name, ws in sheets.items():
                    if sheet_name == "OpenPositions":
                        cols = "BCDFGH"
                    elif sheet_name == "Summary":
                        continue
                    else:
                        cols = "BCDFG"
                    for letter in cols:
                        ws.set_column(f"{letter}:{letter}", None, num_format)
                    last_row: int = last_row_idxs[sheet_name]
                    ws.set_row(last_row, None, bold_format)
                today = datetime.date.today()
                day_wise_data = await TradeExit.filter(
                    position__subscription__account=account,
                    exit_trade__timestamp__gt=today.replace(day=1)
                ).annotate(
                    sum_pnl=Sum('position__pnl'),
                    sum_charges=Sum('position__charges'),
                    day=Extract("DAY", F("tradeexit__exit_trade\".\"timestamp"))
                ).group_by('day').order_by('day').values('day', 'sum_pnl', 'sum_charges')
                days = len(day_wise_data)
                cols = days + 1
                this_month = today.strftime("%Y-%b")
                blank_row = np.repeat("", cols)
                days_row = [f"{data['day']}-{today.strftime('%b-%y')}" for data in day_wise_data]
                investment: float = await Investment.filter(account=account).annotate(
                    sum_investment=Sum('amount')
                ).first().values_list('sum_investment', flat=True)
                pnl_rows = np.array([
                    np.array(["Gross PnL", *days_row]),
                    *[np.hstack(((expiry), np.repeat(returns['pnl'], days))) for expiry, returns in summary.items() if expiry != this_month],
                    np.array([this_month, *np.array([data['sum_pnl'] for data in day_wise_data]).cumsum()])
                ])
                total_pnl_rows = pnl_rows[1:,1:].astype(np.int64).sum(axis=0)
                cost_rows = np.array([
                    np.array(["Costs", *days_row]),
                    *[np.hstack(((expiry), np.repeat(returns['cost'], days))) for expiry, returns in summary.items() if expiry != this_month],
                    np.array([this_month, *np.array([data['sum_charges'] for data in day_wise_data]).cumsum()])
                ])
                total_cost_rows = cost_rows[1:,1:].astype(np.int64).sum(axis=0)
                roi_rows = np.array([
                    np.array(["", *days_row]),
                    np.array(["Investment", *np.repeat(investment, days)]),
                    np.array(["Net Profit", *(total_pnl_rows - total_cost_rows)]),
                    np.array(["ROI", *((total_pnl_rows - total_cost_rows) * 100 / investment)]),
                ])
                rows = np.array([
                    blank_row,
                    *pnl_rows,
                    np.array(["Total", *total_pnl_rows]),
                    blank_row,
                    blank_row,
                    *cost_rows,
                    np.array(["Total", *total_cost_rows]),
                    blank_row,
                    blank_row,
                    *roi_rows
                ])
                ws: worksheet.Worksheet = wb.sheetnames["Summary"]
                for i, row in enumerate(rows):
                    ws.write_row(i, 0, row)
        except Exception as ex:
            logging.error(f"Error in pnl generation for {account.id}-{account.name}", exc_info=ex)
        fp.seek(0)
        return fp

    @staticmethod
    async def generate_shadow_positions_excel():
        accounts_q = Subscription.filter(active=True).values('account_id')
        accounts = await Account.filter(id__in=Subquery(accounts_q))
        fp = BytesIO()
        with pd.ExcelWriter(fp) as excel:
            for account in accounts:
                subscription = await Subscription.filter(account=account, is_hedge=False).get()
                sub_data = await SubscriptionData.filter(subscription=subscription).get_or_none()
                if sub_data and 'positions' in sub_data.data:
                    df = pd.DataFrame(sub_data.data['positions'])
                    df['inst_id'] = df['inst_id'].astype('int')
                    data = await Instrument.filter(id__in=df['inst_id'].to_list()).values(inst_id='id', ticker='future__stock__ticker')
                    df2 = pd.DataFrame(data)
                    df = pd.merge(df, df2, on='inst_id')
                    try:
                        df = df[['ticker', 'side', 'qty' , 'entry_time', 'price', 'old_price', 'exit_price', 'exit_time', 'mtm']]
                    except KeyError:
                        pass
                    df.to_excel(excel, sheet_name=account.name, index=False)
        fp.seek(0)
        return fp

    async def run(self):
        await self.save_eod_price()
        account_ids = await Subscription.filter(active=True).values_list('account_id', flat=True)
        accounts = await Account.filter(id__in=account_ids)
        for account in accounts:
            await self.save_pnl(account)
