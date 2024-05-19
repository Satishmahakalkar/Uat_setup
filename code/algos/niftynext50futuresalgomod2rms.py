from algos.niftynext50algopnlrms import NiftyNext50AlgoPnlRMS


class NiftyNext50FuturesAlgoMod2RMS(NiftyNext50AlgoPnlRMS):

    async def init(self, **kwargs):
        await super().init("strategy2mod2", "NiftyNext50", **kwargs)