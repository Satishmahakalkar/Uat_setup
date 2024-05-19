import datetime
import logging
from algos.basealgo import BaseAlgo
from database.models import Account, Algo, Instrument, Ltp, Option, OptionType, Position, Stock, Subscription, Trade, TradeExit, TradeSide
from tortoise.expressions import F


class NiftyOptionHedgeAlgo(BaseAlgo):

    async def init(self, exit_only=False, roll_on_expiry=False):
        self.algo = await Algo.get(name=self.__class__.__name__)
        self.long_risk_percent = 7 / 100
        self.short_risk_percent = 3 / 100
        self.absolute_loss_percent = 50 / 100
        self.exit_only = exit_only
        self.roll_on_expiry = roll_on_expiry

    def risk_percent(self, net_exposure):
        if net_exposure > 0:
            return self.long_risk_percent
        else:
            return self.short_risk_percent

    async def get_price(self, instrument: Instrument) -> float:
        ltp = await Ltp.filter(instrument=instrument).get()
        return ltp.price

    async def get_nifty_price(self):
        instrument = await Instrument.filter(stock__ticker='NIFTY 50', stock__is_index=True).get()
        return await self.get_price(instrument)

    async def get_nifty_option_price(self, opt: Option):
        instrument = await Instrument.filter(option=opt).get()
        return await self.get_price(instrument)

    async def get_nearest_nifty_option(self, nifty_price, exposure) -> Option:
        today = datetime.date.today()
        stock = await Stock.filter(ticker='NIFTY 50', is_index=True).get()
        opt = await Option.filter(stock=stock, expiry__gt=today).order_by('expiry').first()
        if not opt:
            raise ValueError("Option not available")
        expiry = opt.expiry
        values = await Option.filter(stock=stock, expiry=expiry).annotate(
            diff_strike = F('strike') - nifty_price
        ).values('diff_strike', 'strike')
        values = filter(lambda value: value['strike'] % 100 == 0, values)
        strike = min(values, key = lambda value: abs(value['diff_strike']))['strike']
        option_type = OptionType.CALL if exposure < 0 else OptionType.PUT
        opt = await Option.filter(
            stock=stock,
            strike=strike,
            expiry=expiry,
            option_type=option_type
        ).get()
        return opt
    
    async def should_rollover(self, position: Position) -> bool:
        if self.roll_on_expiry and position:
            option = position.instrument.option
            if option.expiry > datetime.date.today():
                return False
        return True

    async def run(self):
        account_ids = await Subscription.filter(algo=self.algo, active=True).values_list('account_id', flat=True)
        accounts = await Account.filter(id__in=account_ids)
        for account in accounts:
            sub = await Subscription.filter(account=account, algo=self.algo).get()
            position = await Position.filter(subscription=sub, active=True).select_related('instrument__option').get_or_none()
            if not self.should_rollover(position):
                continue
            subs = await Subscription.filter(account=account, active=True, is_hedge=False).values_list('id', flat=True)
            positions = await Position.filter(subscription_id__in=subs, active=True).select_related('instrument')
            price_qty = []
            for pos in positions:
                price = await self.get_price(pos.instrument)
                price_qty.append((price, pos.qty))
            net_exposure = sum(price * qty for price, qty in price_qty)
            risk_percent = self.risk_percent(net_exposure)
            max_loss = net_exposure * risk_percent
            max_loss_absolute = max_loss * self.absolute_loss_percent
            expected_protection = max_loss - max_loss_absolute
            nifty_spot_price = await self.get_nifty_price()
            implied_strike = nifty_spot_price * (1 - risk_percent)
            opt2 = await self.get_nearest_nifty_option(nifty_spot_price, net_exposure)
            opt1 = await self.get_nearest_nifty_option(implied_strike, net_exposure)
            opt2_price = await self.get_nifty_option_price(opt2)
            opt1_price = await self.get_nifty_option_price(opt1)
            expected_profit_per_qty = opt2_price - opt1_price
            expected_profit_per_lot = expected_profit_per_qty * opt1.lot_size
            lots = round(expected_protection / expected_profit_per_lot)
            if position and position.instrument.option != opt1:
                try:
                    price = await self.get_nifty_option_price(position.instrument)
                    await self.exit(position, price)
                except Exception as ex:
                    logging.error(f"Error in exiting position {position}", exc_info=ex)
                    return
            elif position and position.instrument.option == opt1:
                return
            opt_instrument = await Instrument.filter(option=opt1).get()
            await self.entry(sub, opt_instrument, qty=(opt1.lot_size * lots), side=TradeSide.BUY, price=opt1_price)