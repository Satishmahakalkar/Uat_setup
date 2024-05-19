from algos.shadowanalysis import ShadowAnalysis


class NiftyS2ShadowAnalysis(ShadowAnalysis):

    async def init(self, **kwargs):
        await super().init("strategy2mod2", "Nifty50", **kwargs)