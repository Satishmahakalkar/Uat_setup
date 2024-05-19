from algos.basealgo import BaseAlgoExit


class NiftyFuturesAlgoMod(BaseAlgoExit):

    async def init(self, **kwargs):
        await super().init("strategy2mod", "Nifty50", **kwargs)