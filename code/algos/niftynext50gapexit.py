from algos.niftygapexit import NiftyGapExit


class NiftyNext50GapExit(NiftyGapExit):

    async def init(self, *args, **kwargs):
        await super().init(*args, **kwargs)
        self.index_ticker = "NIFTY NEXT 50"