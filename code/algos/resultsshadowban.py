import logging
from algos.resultshedgealgo import ResultsHedgeAlgo
from database.models import Subscription, SubscriptionData
from tortoise.exceptions import DoesNotExist, MultipleObjectsReturned


class ResultsShadowBan(ResultsHedgeAlgo):

    async def run(self):
        tickers = await self.get_results_stock_names()
        logging.info(f"Results stocks {tickers}")
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