from algos.shadowanalysis import ShadowAnalysis


class NiftyS9ShadowAnalysis(ShadowAnalysis):

    async def init(self, **kwargs):
        await super().init("strategy9", "Nifty50", **kwargs)