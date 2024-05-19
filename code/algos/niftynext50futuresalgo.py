from decimal import Decimal
from algos.basealgo import BaseAlgoExit
from database.models import StockGroupMap


class NiftyNext50FuturesAlgo(BaseAlgoExit):

    async def init(self, **kwargs):
        await super().init("strategy2", "NiftyNext50", **kwargs)

    async def get_investment_per_stock(self, investment):
        total_stocks = await StockGroupMap.filter(stock_group=self.stock_group).count()
        return (investment * 5 / total_stocks) * Decimal(1.10)