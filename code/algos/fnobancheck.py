import logging
import aiohttp
from algos.basealgo import BaseAlgo
from database.models import Algo, Subscription, SubscriptionData
from tortoise.exceptions import MultipleObjectsReturned, DoesNotExist


class FnOBanCheck(BaseAlgo):

    async def init(self):
        self.algo = await Algo.get(name=self.__class__.__name__)

    @staticmethod
    async def get_fno_ban_list() -> list:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://nsearchives.nseindia.com/content/fo/fo_secban.csv", headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.5',
                'Origin': 'https://www.bseindia.com',
                'Connection': 'keep-alive',
                'Referer': 'https://www.bseindia.com/',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-site',
                'Pragma': 'no-cache',
                'Cache-Control': 'no-cache'
            }) as res:
                data = await res.text()
        return [line.split(',')[1] for line in data.splitlines()[1:]]

    async def run(self):
        tickers = await self.get_fno_ban_list()
        logging.info(f"Banned stocks {tickers}")
        tickers_set = set(tickers)
        subs = await Subscription.filter(algo=self.algo, active=True).select_related('account')
        for sub in subs:
            try:
                sub_primary = await Subscription.filter(account=sub.account, active=True, is_hedge=False).get()
                sub_data = await SubscriptionData.filter(subscription=sub_primary).get()
            except (MultipleObjectsReturned, DoesNotExist):
                continue
            sub_data.data['banned_stocks'] = list(set(sub_data.data.get('banned_stocks', [])) | tickers_set)
            await sub_data.save()