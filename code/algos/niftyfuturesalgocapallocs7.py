from algos.basealgo import BaseRMSCapAlloc


class NiftyFuturesAlgoCapAllocS7(BaseRMSCapAlloc):

    async def init(self, **kwargs):
        await super().init("strategy7", "Nifty50", **kwargs)