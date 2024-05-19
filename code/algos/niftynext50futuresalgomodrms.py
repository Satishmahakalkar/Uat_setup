from algos.niftynext50algopnlrms import NiftyNext50AlgoPnlRMS


class NiftyNext50FuturesAlgoModRMS(NiftyNext50AlgoPnlRMS):

    async def init(self, **kwargs):
        await super().init("strategy2mod", "NiftyNext50", **kwargs)