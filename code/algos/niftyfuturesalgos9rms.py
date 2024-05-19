from algos.basealgo import BaseAlgoPnlRMS


class NiftyFuturesAlgoS9RMS(BaseAlgoPnlRMS):

    async def init(self, **kwargs):
        await super().init("strategy9", "Nifty50", **kwargs)