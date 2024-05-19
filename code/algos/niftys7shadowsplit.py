from algos.shadowsplits import ShadowSplit


class NiftyS7ShadowSplit(ShadowSplit):

    async def init(self, **kwargs):
        await super().init("strategy7", "Nifty50", **kwargs)