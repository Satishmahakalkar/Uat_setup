from decimal import Decimal
from algos.basealgo import BaseAlgoExit
from database.models import StockGroupMap


class NiftyBalanceFuturesAlgo(BaseAlgoExit):

    async def init(self, **kwargs):
        await super().init("strategy2", "NiftyNextBalance", **kwargs)

    async def get_investment_per_stock(self, investment):
        total_stocks = await StockGroupMap.filter(stock_group=self.stock_group).count()
        return (investment * 3 / total_stocks) * Decimal(1.10)