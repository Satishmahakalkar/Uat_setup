from algos.basealgo import BaseAlgoExit


class NiftyFuturesAlgoMod2(BaseAlgoExit):

    async def init(self, **kwargs):
        await super().init("strategy2mod2", "Nifty50", **kwargs)