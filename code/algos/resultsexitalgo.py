from algos.resultshedgealgo import ResultsHedgeAlgo
from database.models import Ltp, Position, Subscription
from tortoise.expressions import Subquery


class ResultsExitAlgo(ResultsHedgeAlgo):

    async def run(self):
        tickers = await self.get_results_stock_names()
        account_ids_q = Subscription.filter(algo=self.algo, active=True).values_list('account_id')
        sub_id_q = Subscription.filter(account_id__in=Subquery(account_ids_q), is_hedge=False).values_list('id')
        positions = await Position.filter(
            subscription_id__in=Subquery(sub_id_q),
            active=True,
            instrument__future__stock__ticker__in=tickers
        ).select_related('instrument')
        for position in positions:
            ltp = await Ltp.filter(instrument_id=position.instrument.id).get()
            await self.exit(position, ltp.price)