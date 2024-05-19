from algos.niftypricebandexit import NiftyPriceBandExit


class NiftyNext50PriceBandExit(NiftyPriceBandExit):

    async def init(self, *args, **kwargs):
        await super().init(*args, **kwargs)
        self.index_ticker = "NIFTY NEXT 50"
        self.nifty_movement_allowed = 0.3