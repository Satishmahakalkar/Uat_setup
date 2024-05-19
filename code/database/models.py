import base64
import hashlib
import hmac
import os
import random
import string
from tortoise.models import Model
from tortoise import Tortoise, fields, run_async
from enum import Enum
import settings


class OptionType(Enum):
    CALL = "CE"
    PUT = "PE"


class Interval(Enum):
    EOD = "eod"
    HOUR = "hour"
    MIN_1 = "1min"
    MIN_5 = "5min"
    MIN_30 = "30min"


class TradeSide(Enum):
    BUY = "buy"
    SELL = "sell"


class ClientExcelType(Enum):
    CREST = "tradescsv"
    KOTAK = "tradescsv2"
    KOTAK2 = "tradescsv3"


class Stock(Model):
    ticker = fields.CharField(max_length=20, unique=True)
    name = fields.CharField(max_length=254, null=True)
    isin = fields.CharField(max_length=12)
    is_index = fields.BooleanField(default=False)

    def __str__(self) -> str:
        return self.ticker
    

class Future(Model):
    stock = fields.ForeignKeyField("models.Stock", on_delete=fields.CASCADE)
    expiry = fields.DateField()
    lot_size = fields.IntField()


class Option(Model):
    stock = fields.ForeignKeyField("models.Stock", on_delete=fields.CASCADE)
    strike = fields.IntField()
    expiry = fields.DateField()
    option_type = fields.CharEnumField(OptionType)
    lot_size = fields.IntField()


class Instrument(Model):
    stock = fields.ForeignKeyField("models.Stock", on_delete=fields.CASCADE, null=True)
    future = fields.ForeignKeyField("models.Future", on_delete=fields.CASCADE, null=True)
    option = fields.ForeignKeyField("models.Option", on_delete=fields.CASCADE, null=True)


class Ohlc(Model):
    instrument = fields.ForeignKeyField("models.Instrument", on_delete=fields.CASCADE)
    timestamp = fields.DatetimeField()
    interval = fields.CharEnumField(Interval)
    open = fields.FloatField()
    high = fields.FloatField()
    low = fields.FloatField()
    close = fields.FloatField()

    class Meta:
        unique_together = ('instrument', 'timestamp', 'interval')


class Ltp(Model):
    instrument = fields.ForeignKeyField("models.Instrument", on_delete=fields.CASCADE, unique=True)
    price = fields.FloatField()
    timestamp = fields.DatetimeField(auto_now=True)


class StockOldName(Model):
    stock = fields.ForeignKeyField("models.Stock", on_delete=fields.CASCADE)
    ticker = fields.CharField(max_length=20)
    name = fields.CharField(max_length=254, null=True)


class StockGroup(Model):
    name = fields.CharField(max_length=20)


class StockGroupMap(Model):
    stock_group = fields.ForeignKeyField("models.StockGroup", on_delete=fields.CASCADE)
    stock = fields.ForeignKeyField("models.Stock", on_delete=fields.CASCADE)


class User(Model):
    email = fields.CharField(max_length=254, unqiue=True)
    registeration_time = fields.DatetimeField(auto_now_add=True)

    def __str__(self) -> str:
        return self.email


class Account(Model):
    start_date = fields.DateField(auto_new_add=True)
    user = fields.ForeignKeyField("models.User", on_delete=fields.CASCADE)
    name = fields.CharField(max_length=120, default="")


class Strategy(Model):
    name = fields.CharField(max_length=120)


class Algo(Model):
    name = fields.CharField(max_length=120)


class Subscription(Model):
    account = fields.ForeignKeyField("models.Account", on_delete=fields.CASCADE)
    algo = fields.ForeignKeyField("models.Algo", on_delete=fields.CASCADE)
    is_hedge = fields.BooleanField(default=False)
    start_date = fields.DateField(auto_now_add=True)
    active = fields.BooleanField(default=True)


class SubscriptionData(Model):
    subscription = fields.OneToOneField("models.Subscription", on_delete=fields.CASCADE)
    data = fields.JSONField()


class Investment(Model):
    account = fields.ForeignKeyField("models.Account", on_delete=fields.CASCADE)
    amount = fields.DecimalField(max_digits=13, decimal_places=2)
    timestamp = fields.DatetimeField(auto_now_add=True)


class PnL(Model):
    account = fields.ForeignKeyField("models.Account", on_delete=fields.CASCADE)
    date = fields.DateField(auto_now_add=True)
    investment = fields.DecimalField(max_digits=13, decimal_places=2)
    unrealised_pnl = fields.DecimalField(max_digits=13, decimal_places=2)
    realised_pnl = fields.DecimalField(max_digits=13, decimal_places=2)


class Trade(Model):
    subscription = fields.ForeignKeyField("models.Subscription", on_delete=fields.CASCADE)
    instrument = fields.ForeignKeyField("models.Instrument", on_delete=fields.CASCADE)
    timestamp = fields.DatetimeField(auto_now_add=True)
    side = fields.CharEnumField(TradeSide)
    qty = fields.IntField()
    price = fields.DecimalField(max_digits=13, decimal_places=2)


class TradeExit(Model):
    entry_trade = fields.OneToOneField("models.Trade", on_delete=fields.CASCADE, related_name='trade_exit')
    exit_trade = fields.OneToOneField("models.Trade", on_delete=fields.CASCADE, related_name='trade_entry', null=True)
    position = fields.OneToOneField("models.Position", on_delete=fields.CASCADE)


class Position(Model):
    subscription = fields.ForeignKeyField("models.Subscription", on_delete=fields.CASCADE)
    instrument = fields.ForeignKeyField("models.Instrument", on_delete=fields.CASCADE)
    qty = fields.IntField()
    side = fields.CharEnumField(TradeSide)
    buy_price = fields.DecimalField(max_digits=13, decimal_places=2, null=True)
    sell_price = fields.DecimalField(max_digits=13, decimal_places=2, null=True)
    eod_price = fields.DecimalField(max_digits=13, decimal_places=2, null=True)
    charges = fields.DecimalField(max_digits=13, decimal_places=2)
    pnl = fields.DecimalField(max_digits=13, decimal_places=2)
    active = fields.BooleanField(default=True)
    reversal = fields.BooleanField(default=False)


class TradesMail(Model):
    account = fields.ForeignKeyField("models.Account", on_delete=fields.CASCADE)
    subject = fields.CharField(max_length=254)
    body = fields.TextField()
    html = fields.TextField()
    attachment = fields.BinaryField(null=True, default=None)
    timestamp = fields.DatetimeField(auto_now_add=True)


class SREAccount(Model):
    account = fields.OneToOneField("models.Account", on_delete=fields.CASCADE)


class ClientExcelAccount(Model):
    account = fields.OneToOneField("models.Account", on_delete=fields.CASCADE)
    client_account_id = fields.CharField(max_length=50)
    template_type = fields.CharEnumField(ClientExcelType, default=ClientExcelType.CREST.value)


class SREOrders(Model):
    sre_account = fields.ForeignKeyField("models.SREAccount", on_delete=fields.CASCADE)
    trade = fields.ForeignKeyField("models.Trade", on_delete=fields.CASCADE)
    timestamp = fields.DatetimeField(auto_now_add=True)
    app_order_id = fields.IntField(null=True)
    status = fields.CharField(max_length=10, null=True, default=None)


class AccountEmail(Model):
    account = fields.ForeignKeyField("models.Account", on_delete=fields.CASCADE)
    email = fields.CharField(max_length=254)


class UserAuth(Model):
    user = fields.OneToOneField("models.User", on_delete=fields.CASCADE)
    password = fields.CharField(max_length=120)
    is_admin = fields.BooleanField(default=False)
    last_login = fields.DatetimeField(null=True, default=None)

    async def set_password(self, password: str):
        hash_algo = "sha256"
        iterations = random.randint(10, 100) * 1000
        salt = os.urandom(16)
        hashed_password = hashlib.pbkdf2_hmac(hash_algo, password.encode("utf-8"), salt, iterations, 32)
        b64_salt = base64.b64encode(salt)
        b64_hashed_password = base64.b64encode(hashed_password)
        password = "$".join(["pbkdf2_" + hash_algo, iterations, b64_salt, b64_hashed_password])
        self.password = password
        await self.save()

    def verify_password(self, password: str):
        hash_algo, iterations, b64_salt, b64_hashed_password = self.password.split("$")
        hash_algo = hash_algo[len("pbkdf2_"):]
        hashed_password = base64.b64decode(b64_hashed_password)
        salt = base64.b64decode(b64_salt)
        return hmac.compare_digest(
            password.encode("utf-8"),
            hashlib.pbkdf2_hmac(hash_algo, hashed_password, salt, iterations, 32)
        )