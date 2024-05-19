from decimal import Decimal
from algos.shadowanalysis import ShadowAnalysis


class NiftyNext50S2ShadowAnalysis(ShadowAnalysis):

    async def init(self, **kwargs):
        await super().init("strategy2mod2", "NiftyNext50", **kwargs)

    @staticmethod
    def max_value_at_risk(investment: Decimal) -> float:
        return 300000

    @staticmethod
    def get_mtm_threshold(investment) -> float:
        return (1000000 / 15000000) * float(investment)