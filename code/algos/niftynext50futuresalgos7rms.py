from algos.niftynext50algopnlrms import NiftyNext50AlgoPnlRMS


class NiftyNext50FuturesAlgoS7RMS(NiftyNext50AlgoPnlRMS):

    async def init(self, **kwargs):
        await super().init("strategy7", "NiftyNext50", **kwargs)
