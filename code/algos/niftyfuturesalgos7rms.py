from algos.basealgo import BaseAlgoPnlRMS


class NiftyFuturesAlgoS7RMS(BaseAlgoPnlRMS):

    async def init(self, **kwargs):
        await super().init("strategy7", "Nifty50", **kwargs)