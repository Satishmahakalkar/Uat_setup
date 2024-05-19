from algos.basealgo import BaseAlgoExit
from database.models import *


class NiftyFuturesAlgo(BaseAlgoExit):

    async def init(self, **kwargs):
        await super().init("strategy2", "Nifty50", **kwargs)
