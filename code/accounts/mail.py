import datetime
import importlib
from io import BytesIO
from typing import Dict, List, Tuple
from email.message import EmailMessage
from email.utils import make_msgid
from pathlib import Path
from aiosmtplib import SMTP
from accounts.pnl import PnlSave
from algos.basealgo import BaseAlgo
from algos.shadowanalysis import ShadowAnalysis, ShadowPosition
from database.models import Account, AccountEmail, Algo, ClientExcelAccount, ClientExcelType, Instrument, Ltp, PnL, Position, SREOrders, Subscription, SubscriptionData, Trade, TradeExit, TradeSide, TradesMail, User
from tortoise.expressions import Subquery, Q
import settings
import jinja2


class BaseMailer:

    def __init__(self) -> None:
        template_path = Path.cwd() / 'accounts' / 'mailtemplates'
        self.jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(template_path), enable_async=True)
        self.mails: List[TradesMail] = []

    async def get_symbol(self, instrument: Instrument) -> str:
        await instrument.fetch_related('stock', 'future__stock', 'option__stock')
        if instrument.stock:
            return instrument.stock.ticker
        elif instrument.future:
            return f"{instrument.future.stock.ticker} {instrument.future.expiry.strftime('%b')} FUT"
        else:
            return f"{instrument.option.stock.ticker} {instrument.option.strike} {instrument.option.expiry} {instrument.option.option_type.value}"

    async def send_mails(self):
        smtp = SMTP("smtp-relay.gmail.com", 587)
        await smtp.connect()
        await smtp.ehlo()
        await smtp.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
        for trades_mail in self.mails:
            await trades_mail.fetch_related('account__user')
            emails = await AccountEmail.filter(account=trades_mail.account).values_list('email', flat=True)
            msg = EmailMessage()
            msg['To'] = trades_mail.account.user.email
            msg['CC'] = ",".join(emails)
            msg['From'] = settings.FROM_EMAIL
            msg['Subject'] = trades_mail.subject
            msg.set_content(trades_mail.body)
            msg.set_content(trades_mail.html, subtype='html')
            if trades_mail.attachment:
                msg.add_attachment(trades_mail.attachment.decode(), filename="BasketTrades.csv")
                msg.add_attachment(trades_mail.attachment.decode(), filename="BasketTrades.txt")
            msg['Message-ID'] = make_msgid(domain="algonauts.in")
            await smtp.send_message(msg)
        smtp.close()

    def run(self):
        raise NotImplementedError


class TradesMailer(BaseMailer):

    def __init__(self, algo: BaseAlgo, send_no_trades=True, rollover=False, reverse=False, partial=False) -> None:
        super().__init__()
        self.algo = algo
        self.trades = algo.trades
        self.txt_template = self.jinja_env.get_template("tradesmail.txt")
        self.html_template = self.jinja_env.get_template("tradesmail.html")
        self.no_trades_txt_template = self.jinja_env.get_template("notradesmail.txt")
        self.no_trades_html_template = self.jinja_env.get_template("notradesmail.html")
        self.trades_csv_templates = {
            client_excel_type: self.jinja_env.get_template(f"{client_excel_type.value}.txt")
            for client_excel_type in ClientExcelType
        }
        self.send_no_trades = send_no_trades
        self.rollover = rollover
        self.reverse = reverse
        self.partial = partial

    async def run(self):
        trades = Trade.filter(id__in=[td.id for td in self.trades])
        account_ids = await trades.values_list('subscription__account__id', flat=True)
        algo = await Algo.get(name=self.algo.__class__.__name__)
        subscribed_account_ids = await Subscription.filter(algo=algo, active=True).values_list('account_id', flat=True)
        accounts = await Account.filter(Q(id__in=account_ids) & Q(id__in=subscribed_account_ids)).distinct()
        for account in accounts:
            account_trade_ids = await Trade.filter(
                subscription__account=account, id__in=Subquery(trades.values('id'))
            ).values_list('id', flat=True)
            entry_trade_ids = await TradeExit.filter(entry_trade_id__in=account_trade_ids).values_list('entry_trade_id', flat=True)
            exit_trade_ids = await TradeExit.filter(exit_trade_id__in=account_trade_ids).values_list('exit_trade_id', flat=True)
            long_entrys = await Trade.filter(id__in=entry_trade_ids, side=TradeSide.BUY).select_related('instrument')
            short_entrys = await Trade.filter(id__in=entry_trade_ids, side=TradeSide.SELL).select_related('instrument')
            long_exits = await Trade.filter(id__in=exit_trade_ids, side=TradeSide.SELL).select_related('instrument')
            short_exits = await Trade.filter(id__in=exit_trade_ids, side=TradeSide.BUY).select_related('instrument')
            long_entrys = [(trd.side.value, trd.qty, await self.get_symbol(trd.instrument), trd.price) for trd in long_entrys]
            short_entrys = [(trd.side.value, trd.qty, await self.get_symbol(trd.instrument), trd.price) for trd in short_entrys]
            long_exits = [(trd.side.value, trd.qty, await self.get_symbol(trd.instrument), trd.price) for trd in long_exits]
            short_exits = [(trd.side.value, trd.qty, await self.get_symbol(trd.instrument), trd.price) for trd in short_exits]
            cl_ex_account = await ClientExcelAccount.filter(account=account).get_or_none()
            if cl_ex_account:
                trades_for_acc = await trades.filter(subscription__account=account)
                csv = await self.trades_csv_templates[cl_ex_account.template_type].render_async(
                    account=account,
                    trades=trades_for_acc,
                    client_account_id=cl_ex_account.client_account_id
                )
                attachment = csv.encode()
            else:
                attachment = None
            await account.fetch_related('user')
            txt = await self.txt_template.render_async(
                account=account,
                long_entrys=long_entrys,
                short_entrys=short_entrys,
                long_exits=long_exits,
                short_exits=short_exits
            )
            html = await self.html_template.render_async(
                account=account,
                long_entrys=long_entrys,
                short_entrys=short_entrys,
                long_exits=long_exits,
                short_exits=short_exits
            )
            subject = f"Trades for {datetime.date.today()}"
            if self.rollover:
                subject = "Rollover " + subject
            elif self.reverse:
                subject = "Reversal " + subject
            elif self.partial:
                subject = "Partial " + subject
            trades_mail = await TradesMail.create(
                account=account,
                subject=subject,
                body=txt,
                html=html,
                attachment=attachment
            )
            self.mails.append(trades_mail)
        if self.send_no_trades:
            no_trades_accounts = await Account.filter(id__in=subscribed_account_ids).exclude(id__in=account_ids)
            for account in no_trades_accounts:
                await account.fetch_related('user')
                txt = await self.no_trades_txt_template.render_async(account=account)
                html = await self.no_trades_html_template.render_async(account=account)
                trades_mail = await TradesMail.create(
                    account=account,
                    subject=f"Trades for {datetime.date.today()}",
                    body=txt,
                    html=html
                )
                self.mails.append(trades_mail)
        await self.send_mails()


class PositionsMailer(BaseMailer):

    def __init__(self) -> None:
        super().__init__()
        self.txt_template = self.jinja_env.get_template("positions.txt")
        self.html_template = self.jinja_env.get_template("positions.html")

    async def run_for_account(self, account: Account):
        long_positions = await Position.filter(subscription__account=account, active=True, side=TradeSide.BUY).select_related('instrument')
        short_positions = await Position.filter(subscription__account=account, active=True, side=TradeSide.SELL).select_related('instrument')
        long_positions = [(await self.get_symbol(pos.instrument), pos) for pos in long_positions]
        short_positions = [(await self.get_symbol(pos.instrument), pos) for pos in short_positions]
        today = datetime.date.today()
        txt = await self.txt_template.render_async(
            long_positions=long_positions,
            short_positions=short_positions,
            account=account,
            date=today
        )
        html = await self.html_template.render_async(
            long_positions=long_positions,
            short_positions=short_positions,
            account=account,
            date=today
        )
        trades_mail = await TradesMail.create(
            account=account,
            subject=f"Positions for {today}",
            body=txt,
            html=html
        )
        self.mails.append(trades_mail)

    async def run(self):
        account_ids = await Subscription.filter(active=True).values_list('account_id', flat=True)
        accounts = await Account.filter(id__in=account_ids).select_related('user')
        for account in accounts:
            await self.run_for_account(account)
        await self.send_mails()


class PnlMailer(BaseMailer):

    def __init__(self) -> None:
        super().__init__()
        self.attachments: List[Tuple[Account, BytesIO]] = []

    async def run(self):
        account_ids = await Subscription.filter(active=True).values_list('account_id', flat=True)
        accounts = await Account.filter(id__in=account_ids)
        for account in accounts:
            fp = await PnlSave.generate_pnl_excel(account)
            self.attachments.append((account, fp))
        await self.send_mails()

    async def send_mails(self):
        smtp = SMTP("smtp-relay.gmail.com", 587)
        await smtp.connect()
        await smtp.ehlo()
        await smtp.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
        today = datetime.date.today()
        for account, fp in self.attachments:
            await account.fetch_related('user')
            msg = EmailMessage()
            # emails = await AccountEmail.filter(account=account).values_list('email', flat=True)
            # msg['To'] = account.user.email
            # msg['CC'] = ",".join(emails)
            msg['To'] = settings.FROM_EMAIL
            msg['CC'] = ",".join(settings.DEFAULT_RECEIVERS)
            msg['From'] = settings.FROM_EMAIL
            msg['Subject'] = f"PnL for {today}"
            msg.set_content(f"PFA PnL for {account.name}")
            msg.add_attachment(fp.read(), maintype="application", subtype="xlsx", filename=f"PnL_{account.name}_{today}.xlsx")
            msg['Message-ID'] = make_msgid(domain="algonauts.in")
            await smtp.send_message(msg)
        smtp.close()


class ShadowPositionsMailer(BaseMailer):

    def __init__(self) -> None:
        super().__init__()
        self.attachment = None

    async def run(self):
        fp = await PnlSave.generate_shadow_positions_excel()
        self.attachment = fp
        await self.send_mails()

    async def send_mails(self):
        smtp = SMTP("smtp-relay.gmail.com", 587)
        await smtp.connect()
        await smtp.ehlo()
        await smtp.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
        today = datetime.date.today()
        msg = EmailMessage()
        msg['To'] = settings.FROM_EMAIL
        msg['CC'] = ",".join(settings.DEFAULT_RECEIVERS)
        msg['From'] = settings.FROM_EMAIL
        msg['Subject'] = f"Shadow Positions for {today}"
        msg.set_content(f"PFA Shadow Positions")
        msg.add_attachment(self.attachment.read(), maintype="application", subtype="xlsx", filename="PnL.xlsx")
        msg['Message-ID'] = make_msgid(domain="algonauts.in")
        await smtp.send_message(msg)
        smtp.close()


class SRETradesMailer(BaseMailer):

    def __init__(self, sre_orders: List[SREOrders]) -> None:
        super().__init__()
        self.sre_orders = sre_orders
        self.txt_template = self.jinja_env.get_template("sreordersmail.txt")
        self.html_template = self.jinja_env.get_template("sreordersmail.html")
        self.mails: Tuple[str, str] = ()

    async def run(self):
        sre_orders_q = SREOrders.filter(id__in=[sre_order.id for sre_order in self.sre_orders])
        sre_orders = await sre_orders_q.select_related(
            'sre_account__account',
            'trade__instrument__stock',
            'trade__instrument__future__stock',
            'trade__instrument__option__stock'
        )
        txt = await self.txt_template.render_async(sre_orders=sre_orders)
        html = await self.html_template.render_async(sre_orders=sre_orders)
        self.mails = (txt, html)
        await self.send_mails()

    async def send_mails(self):
        smtp = SMTP("smtp-relay.gmail.com", 587)
        await smtp.connect()
        await smtp.ehlo()
        await smtp.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
        today = datetime.date.today()
        msg = EmailMessage()
        msg['To'] = settings.FROM_EMAIL
        msg['CC'] = ",".join(settings.DEFAULT_RECEIVERS)
        msg['From'] = settings.FROM_EMAIL
        msg['Subject'] = f"SRE Orders Placed - {today}"
        msg['Message-ID'] = make_msgid(domain="algonauts.in")
        txt, html = self.mails
        msg.set_content(txt)
        msg.set_content(html, subtype='html')
        await smtp.send_message(msg)
        smtp.close()


class ShadowTradeBasketMailer(BaseMailer):

    def __init__(self) -> None:
        super().__init__()
        self.client_excel_type = ClientExcelType.KOTAK2
        self.trades_csv_template = self.jinja_env.get_template(f"{self.client_excel_type.value}.txt")
        self.mails: List[EmailMessage] = []

    async def create_email_msg(self, client_excel_account: ClientExcelAccount, msg: EmailMessage, attachment_name: str, trades: List[Trade]):
        csv = await self.trades_csv_template.render_async(
            account=client_excel_account.account,
            trades=trades,
            client_account_id=client_excel_account.client_account_id
        )
        msg.add_attachment(csv, filename=f"{attachment_name}.txt")
        return msg

    async def run(self):
        algos_q = Algo.filter(
            name__in=["NiftyS2ShadowAnalysis", "NiftyS7ShadowAnalysis", "NiftyNext50S2ShadowAnalysis", "NiftyNext50S7ShadowAnalysis"]
        ).values('id')
        accounts_q = ClientExcelAccount.filter(template_type=self.client_excel_type).values('account_id')
        subs_q = Subscription.filter(
            account_id__in=Subquery(accounts_q),
            algo_id__in=Subquery(algos_q),
            is_hedge=False
        ).values('id')
        subdatas = await SubscriptionData.filter(
            subscription_id__in=Subquery(subs_q)
        ).select_related('subscription__account', 'subscription__algo')
        for sub_data in subdatas:
            account: Account = sub_data.subscription.account
            module = importlib.import_module(f'algos.{sub_data.subscription.algo.name.lower()}')
            algo_strat_class = getattr(module, sub_data.subscription.algo.name)
            algo_strat: ShadowAnalysis = algo_strat_class()
            await algo_strat.init()
            shadow_positions: List[ShadowPosition] = sub_data.data['positions']
            longs, shorts, longs_reverse, shorts_reverse, longs_partial, shorts_partial, longs_partial_reverse, shorts_partial_reverse, ongoing_entry, ongoing_exit = [], [], [], [], [], [], [], [], [], []
            for shadow_position in shadow_positions:
                if shadow_position.get('exit_time'):
                    continue
                instrument = await Instrument.filter(id=shadow_position['inst_id']).get()
                side = TradeSide(shadow_position['side'])
                opposite_side = TradeSide.SELL if side == TradeSide.BUY else TradeSide.BUY
                qty = await algo_strat.get_qty(instrument, account)
                qty_partial = await algo_strat.get_qty_partial(instrument, account)
                trade = Trade(
                    subscription=sub_data.subscription,
                    instrument=instrument,
                    side=side,
                    qty=qty
                )
                reverse_trade = Trade(
                    subscription=sub_data.subscription,
                    instrument=instrument,
                    side=opposite_side,
                    qty=qty
                )
                partial_trade = Trade(
                    subscription=sub_data.subscription,
                    instrument=instrument,
                    side=side,
                    qty=qty_partial
                )
                reverse_partial_trade = Trade(
                    subscription=sub_data.subscription,
                    instrument=instrument,
                    side=opposite_side,
                    qty=qty_partial
                )
                if side == TradeSide.BUY:
                    longs.append(trade)
                    longs_reverse.append(reverse_trade)
                    longs_partial.append(partial_trade)
                    longs_partial_reverse.append(reverse_partial_trade)
                else:
                    shorts.append(trade)
                    shorts_reverse.append(reverse_trade)
                    shorts_partial.append(partial_trade)
                    shorts_partial_reverse.append(reverse_partial_trade)
                position = await Position.filter(
                    instrument=instrument,
                    subscription=sub_data.subscription,
                    active=True
                ).get_or_none()
                if position and position.side != side:
                    ongoing_exit.append(trade)
                if not position or position.side != side:
                    ongoing_entry.append(trade)
            positions = await Position.filter(
                subscription=sub_data.subscription,
                active=True
            ).exclude(
                instrument_id__in=(shadow_position['inst_id'] for shadow_position in shadow_positions)
            ).select_related('instrument')
            for position in positions:
                opposite_side = TradeSide.SELL if side == TradeSide.BUY else TradeSide.BUY
                trade = Trade(
                    subscription=sub_data.subscription,
                    instrument=position.instrument,
                    side=opposite_side,
                    qty=position.qty
                )
                ongoing_exit.append(trade)
            client_excel_account = await ClientExcelAccount.filter(
                account=account,
                template_type=self.client_excel_type
            ).select_related('account').get()
            emails = await AccountEmail.filter(account=account).values_list('email', flat=True)
            msg = EmailMessage()
            user = await account.user
            msg['To'] = user.email
            msg['CC'] = ",".join(emails)
            msg['From'] = settings.FROM_EMAIL
            msg['Subject'] = "Trade Baskets"
            msg['Message-ID'] = make_msgid(domain="algonauts.in")
            msg.set_content(f"PFA trade basket templates for {account.name}")
            await self.create_email_msg(client_excel_account, msg, "LongTrades", longs)
            await self.create_email_msg(client_excel_account, msg, "LongReversalTrades", longs_reverse)
            await self.create_email_msg(client_excel_account, msg, "ShortTrades", shorts)
            await self.create_email_msg(client_excel_account, msg, "ShortReversalTrades", shorts_reverse)
            await self.create_email_msg(client_excel_account, msg, "LongExitTrades", longs_reverse)
            await self.create_email_msg(client_excel_account, msg, "LongReversalExitTrades", longs)
            await self.create_email_msg(client_excel_account, msg, "ShortExitTrades", shorts_reverse)
            await self.create_email_msg(client_excel_account, msg, "ShortReversalExitTrades", shorts)
            await self.create_email_msg(client_excel_account, msg, "LongPartialTrades", longs_partial)
            await self.create_email_msg(client_excel_account, msg, "ShortPartialTrades", shorts_partial)
            await self.create_email_msg(client_excel_account, msg, "LongPartialExitTrades", longs_partial_reverse)
            await self.create_email_msg(client_excel_account, msg, "ShortPartialExitTrades", shorts_partial_reverse)
            await self.create_email_msg(client_excel_account, msg, "LongPartialReversalTrades", longs_partial_reverse)
            await self.create_email_msg(client_excel_account, msg, "ShortPartialReversalTrades", shorts_partial_reverse)
            await self.create_email_msg(client_excel_account, msg, "LongPartialReversalExitTrades", longs_partial)
            await self.create_email_msg(client_excel_account, msg, "ShortPartialReversalExitTrades", shorts_partial)
            await self.create_email_msg(client_excel_account, msg, "OnGoingEntry-9-45-Trades", ongoing_entry)
            await self.create_email_msg(client_excel_account, msg, "OnGoingExit-9-45-Trades", ongoing_exit)
            self.mails.append(msg)
        await self.send_mails()
            
    async def send_mails(self):
        smtp = SMTP("smtp-relay.gmail.com", 587)
        await smtp.connect()
        await smtp.ehlo()
        await smtp.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
        for msg in self.mails:
            await smtp.send_message(msg)
        smtp.close()


class TradeSplitMailer(ShadowTradeBasketMailer):

    async def create_baskets_mail(self, accounts: List[Account], trade_basket_map: Dict[str, List[Trade]]):
        self.mails = []
        for account in accounts:
            client_excel_account = await ClientExcelAccount.filter(
                account=account,
                template_type=self.client_excel_type
            ).select_related('account').get_or_none()
            if not client_excel_account:
                continue
            emails = await AccountEmail.filter(account=account).values_list('email', flat=True)
            msg = EmailMessage()
            user = await account.user
            msg['To'] = user.email
            msg['CC'] = ",".join(emails)
            msg['From'] = settings.FROM_EMAIL
            msg['Subject'] = "Trade Baskets Split Algo"
            msg['Message-ID'] = make_msgid(domain="algonauts.in")
            msg.set_content(f"PFA trade basket templates for {account.name}")
            for name, trades in trade_basket_map.items():
                await self.create_email_msg(client_excel_account, msg, name, trades)
            self.mails.append(msg)
        await self.send_mails()

    async def create_trades_mails(self, trades: List[Trade], subject_tag: str):
        self.mails = []
        today = datetime.date.today()
        trades = await Trade.filter(
            id__in=[td.id for td in trades]
        ).select_related('instrument__future', 'subscription__account')
        accounts = set(td.subscription.account for td in trades)
        for account in accounts:
            emails = await AccountEmail.filter(account=account).values_list('email', flat=True)
            msg = EmailMessage()
            user = await account.user
            msg['To'] = user.email
            msg['CC'] = ",".join(emails)
            msg['From'] = settings.FROM_EMAIL
            msg['Subject'] = f"{subject_tag} Trades for {today} for Split Algo"
            msg['Message-ID'] = make_msgid(domain="algonauts.in")
            msg.set_content("\n".join([
                f"{td.side.name} {td.qty} of {td.instrument.future.stock} {td.instrument.future.expiry}" for td in trades
            ]))
        await self.send_mails()