from algos.basealgo import BaseAlgoPnlRMS


class NiftyFuturesAlgoMod2RMS(BaseAlgoPnlRMS):

    async def init(self, **kwargs):
        await super().init("strategy2mod2", "Nifty50", **kwargs)