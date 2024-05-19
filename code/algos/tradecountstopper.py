import datetime

import pandas as pd
from algos.basealgo import BaseAlgo
from database.models import Account, Algo, Subscription, SubscriptionData, TradeExit
from tortoise.expressions import Subquery

import settings


class TradeCountStopper(BaseAlgo):

    async def init(self):
        self.algo = await Algo.get(name=self.__class__.__name__)

    def get_nth_day_back(self, n: int):
        today = datetime.date.today()
        dates = [today - pd.offsets.BusinessDay(n=(i + 1)) for i in range(n)]
        for date in dates.copy():
            if date in settings.HOLIDAY_DATES:
                dates.append(min(dates) - pd.offsets.BusinessDay(n=1))
        return min(dates)

    async def run(self):
        accounts_q = Subscription.filter(algo=self.algo, active=True).values('account_id')
        accounts = await Account.filter(id__in=Subquery(accounts_q))
        five_days_back = self.get_nth_day_back(5)
        for account in accounts:
            sub = await Subscription.filter(account=account, active=True, is_hedge=False).get()
            subdata = await SubscriptionData.filter(subscription=sub).get()
            trade_exits = await TradeExit.filter(
                entry_trade__subscription=sub,
                entry_trade__timestamp__gte=five_days_back
            ).select_related('entry_trade', 'exit_trade', 'position')
            normal_trades, reversal_trades = [], []
            for trade_exit in trade_exits:
                if trade_exit.position.reversal:
                    reversal_trades.append(trade_exit)
                else:
                    normal_trades.append(trade_exit)
            normal_successful, reverse_successful = 0, 0
            for trade_exit in normal_trades:
                if (
                    (not trade_exit.exit_trade) 
                    or (trade_exit.entry_trade.timestamp.date() != trade_exit.exit_trade.timestamp.date())
                ):
                    normal_successful += 1
            for trade_exit in reversal_trades:
                if trade_exit.position.pnl > 0.0:
                    reverse_successful += 1
            ratio = (normal_successful + reverse_successful) / len(trade_exits)
            subdata.data['trade_counter_ratio'] = ratio
            # if ratio > 0.3:
            #     subdata.data['trade_allowed'] = True
            # else:
            #     subdata.data['trade_allowed'] = False
            await subdata.save()
