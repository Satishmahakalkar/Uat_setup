from algos.shadowanalysis import ShadowAnalysis


class NiftyS7ShadowAnalysis(ShadowAnalysis):

    async def init(self, **kwargs):
        await super().init("strategy7", "Nifty50", **kwargs)