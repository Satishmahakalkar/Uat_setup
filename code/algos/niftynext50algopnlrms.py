from decimal import Decimal
from algos.basealgo import BaseAlgoPnlRMS
from database.models import Algo, StockGroupMap


class NiftyNext50AlgoPnlRMS(BaseAlgoPnlRMS):

    async def init(self, *args, **kwargs) -> None:
        await super().init(*args, **kwargs)
        self.nifty_gap_exit_algo = await Algo.get(name="NiftyNext50GapExit")

    async def get_investment_per_stock(self, investment):
        total_stocks = await StockGroupMap.filter(stock_group=self.stock_group).count()
        return (investment * 5 / total_stocks) * Decimal(1.10)