from algos.basealgo import BaseAlgoExit


class NiftyFuturesAlgoS7(BaseAlgoExit):

    async def init(self, **kwargs):
        await super().init("strategy7", "Nifty50", **kwargs)