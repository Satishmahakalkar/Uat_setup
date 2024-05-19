import datetime
from decimal import Decimal
import logging
from typing import List, Literal
from accounts.killswitch import exit_all_trades, exit_trades_for_account, reverse_trade_exit, send_trades_from_shadow, reverse_trades
from apiserver.utils import JWTAuthBackend, serialize
from dataaggregator.truedata.datasaver import TrueData
import settings
from starlette.applications import Starlette
from starlette.authentication import requires
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.requests import Request
from starlette.exceptions import HTTPException
import jwt
from database.models import *
from tortoise.contrib.starlette import register_tortoise
from tortoise.exceptions import DoesNotExist
from tortoise.expressions import Q, Subquery


async def alive(request: Request):
    return JSONResponse({"status": "alive", "version": "0.1"})


async def jwt_test(request: Request):
    encoded_jwt = jwt.encode({"sub": "test@test.com"}, settings.SECRET, algorithm="HS256")
    return JSONResponse({"jwt": encoded_jwt})


@requires('authenticated')
async def get_accounts(request: Request):
    account_id_q = Subscription.filter(active=True).distinct().values('account_id')
    accounts = await Account.filter(id__in=Subquery(account_id_q)).values('id', 'name')
    return JSONResponse(dict(accounts=accounts))


@requires('authenticated')
async def get_algos(request: Request):
    algos = await Algo.all().values()
    return JSONResponse(dict(algos=algos))


@requires('authenticated')
async def get_pnl(request: Request):
    try:
        account_id = request.path_params['account_id']
        account = await Account.get(id=int(account_id))
        mode: Literal["open", "closed"] = request.path_params['mode']
        if mode == "closed":
            month = int(request.query_params['month'])
            year = int(request.query_params['year'])
    except (KeyError, DoesNotExist, ValueError):
        raise HTTPException(status_code=404)
    trade_exits = TradeExit.filter(position__subscription__account=account)
    if mode == "open":
        data = await trade_exits.filter(position__active=True).values(
            future_stock_name = 'position__instrument__future__stock__ticker',
            option_stock_name = 'position__instrument__option__stock__ticker',
            strike = 'position__instrument__option__strike',
            option_expiry = 'position__instrument__option__expiry',
            future_expiry = 'position__instrument__future__expiry',
            qty = 'position__qty',
            buy_price = 'position__buy_price',
            sell_price = 'position__sell_price',
            side = 'position__side',
            charges = 'position__charges',
            cmp = 'position__eod_price',
            mtm = 'position__pnl',
            entry_time = 'entry_trade__timestamp',
        )
    elif mode == "closed":
        date_start = datetime.date(year, month, 1)
        date_end = datetime.date(year, month + 1, 1) if month < 12 else datetime.date(year + 1, 1, 1)
        data = await trade_exits.filter(position__active=False).filter(
            Q(position__instrument__option__expiry__gt=date_start, position__instrument__option__expiry__lt=date_end)
            | Q(position__instrument__future__expiry__gt=date_start, position__instrument__future__expiry__lt=date_end)
        ).values(
            future_stock_name = 'position__instrument__future__stock__ticker',
            option_stock_name = 'position__instrument__option__stock__ticker',
            strike = 'position__instrument__option__strike',
            option_expiry = 'position__instrument__option__expiry',
            future_expiry = 'position__instrument__future__expiry',
            qty = 'position__qty',
            buy_price = 'position__buy_price',
            sell_price = 'position__sell_price',
            side = 'position__side',
            exit_price = 'exit_trade__price',
            charges = 'position__charges',
            pnl = 'position__pnl',
            entry_time = 'entry_trade__timestamp',
            exit_time = 'exit_trade__timestamp',
        )
    else:
        raise HTTPException(status_code=400)
    for value_dict in data:
        value_dict['stock_name'] = value_dict.get('future_stock_name', value_dict.get('option_stock_name', None))
        value_dict.pop('future_stock_name', None)
        value_dict.pop('option_stock_name', None)
        value_dict['expiry'] = value_dict.get('future_expiry', value_dict.get('option_expiry', None))
        value_dict.pop('future_expiry', None)
        value_dict.pop('option_expiry', None)
    return JSONResponse(dict(pnl=serialize(data)))


@requires('authenticated')
async def get_shadow(request: Request):
    try:
        account_id = request.path_params['account_id']
        account = await Account.get(id=int(account_id))
        sub_data = await SubscriptionData.filter(subscription__account=account, subscription__is_hedge=False).get()
    except DoesNotExist:
        raise HTTPException(status_code=404)
    shadow_positions = sub_data.data.get('positions', [])
    for values in shadow_positions:
        instrument = await Instrument.filter(
            id=values['inst_id']
        ).select_related('future__stock', 'option__stock', 'stock').get()
        if instrument.future:
            stock_name = instrument.future.stock.ticker
        elif instrument.option:
            stock_name = instrument.option.stock.ticker
        else:
            stock_name = instrument.stock.ticker
        values['stock_name'] = stock_name
    return JSONResponse(dict(shadow_positions=serialize(shadow_positions)))


@requires('authenticated')
async def get_prices(request: Request):
    try:
        mode: Literal["eq", "fo"] = request.path_params['mode']
        assert mode in ("eq", "fo")
    except (KeyError, AssertionError):
        raise HTTPException(status_code=404)
    data_saver = TrueData()
    await data_saver.login()
    df = await data_saver.get_bhavcopy(segment=mode)
    data = df.to_dict(orient="records")
    return JSONResponse(dict(prices=data))


@requires('authenticated')
async def kill_switch(request: Request):
    data = await request.json()
    print(data)
    account_id = data.get('account_id')
    account = await Account.filter(id=int(account_id)).get_or_none()
    side = data.get('side')
    if side and account:
        await exit_trades_for_account(account, TradeSide(side))
    elif not side and account:
        await exit_trades_for_account(account)
    elif side and not account:
        await exit_all_trades(side)
    elif not side and not account:
        await exit_all_trades()
    return JSONResponse(dict(status="success"))


@requires('authenticated')
async def reverse(request: Request):
    data = await request.json()
    try:
        account_id = data['account_id']
        account = await Account.get(id=int(account_id))
        side = TradeSide(data['side'])
    except DoesNotExist:
        raise HTTPException(status_code=404)
    await reverse_trades(account, TradeSide(side))
    return JSONResponse(dict(status="success"))


@requires('authenticated')
async def reverse_exit(request: Request):
    data = await request.json()
    try:
        account_id = data['account_id']
        account = await Account.get(id=int(account_id))
        side = TradeSide(data['side'])
    except DoesNotExist:
        raise HTTPException(status_code=404)
    await reverse_trade_exit(account, side)
    return JSONResponse(dict(status="success"))


@requires('authenticated')
async def send_trades(request: Request):
    data = await request.json()
    print(data)
    try:
        account_id = data['account_id']
        account = await Account.get(id=int(account_id))
    except DoesNotExist:
        raise HTTPException(status_code=404)
    side = data.get('side')
    if side:
        await send_trades_from_shadow(account, TradeSide(side))
    else:
        await send_trades_from_shadow(account)
    return JSONResponse(dict(status="success"))


@requires('authenticated')
async def create_account(request: Request):
    try:
        async with request.form() as data:
            logging.info(data)
            algo = await Algo.get(name=data['primary_algo'])
            user, _ = await User.get_or_create(email=data['email'])
            account, _ = await Account.get_or_create(user=user, name=data['name'])
            investment, _ = await Investment.get_or_create(account=account, amount=0)
            investment.amount = data['investment']
            _, created = await Subscription.get_or_create(
                account=account,
                algo=algo,
                defaults=dict(
                    start_date=datetime.date.today(),
                    is_hedge=False
                )
            )
            algos = await Algo.filter(name__in=data.getlist('secondary_algos'))
            for algo in algos:
                await Subscription.get_or_create(
                    account=account,
                    algo=algo,
                    defaults=dict(
                        start_date=datetime.date.today(),
                        is_hedge=True
                    )
                )
            await investment.save()
    except KeyError:
        raise HTTPException(status_code=404)
    return JSONResponse(dict(account_id=account.id, new_account=created))


def make_app():
    routes = [
        Route("/", alive),
        Route("/accounts", get_accounts),
        Route("/accounts/{account_id:int}/pnl/{mode}", get_pnl),
        Route("/accounts/{account_id:int}/shadow", get_shadow),
        Route("/algos", get_algos),
        Route("/prices/{mode}", get_prices),
        Route("/kill-switch", kill_switch, methods=["POST"]),
        Route("/reverse-trades", reverse, methods=["POST"]),
        Route("/reverse-exit", reverse_exit, methods=["POST"]),
        Route("/create-account", create_account, methods=["POST"]),
        Route("/send-trades", send_trades, methods=["POST"]),
    ]
    middleware = [
        Middleware(AuthenticationMiddleware, backend=JWTAuthBackend())
    ]
    app = Starlette(routes=routes, middleware=middleware)
    register_tortoise(app, settings.TORTOISE_ORM)
    return app
