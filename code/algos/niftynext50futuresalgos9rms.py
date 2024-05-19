from algos.niftynext50algopnlrms import NiftyNext50AlgoPnlRMS


class NiftyNext50FuturesAlgoS9RMS(NiftyNext50AlgoPnlRMS):

    async def init(self, **kwargs):
        await super().init("strategy9", "NiftyNext50", **kwargs)