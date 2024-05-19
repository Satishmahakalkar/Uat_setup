from algos.basealgo import BaseAlgoPnlRMS


class NiftyFuturesAlgoModRMS(BaseAlgoPnlRMS):

    async def init(self, **kwargs):
        await super().init("strategy2mod", "Nifty50", **kwargs)