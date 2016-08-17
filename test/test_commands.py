import json
import os
import time
import unittest
from ledger import Amount
from ledger import Balance

from jsonschema import validate
from kraken_manager import Kraken

from sqlalchemy_models import get_schemas, wallet as wm, exchange as em

from trade_manager.helper import start_test_man, stop_test_man
from trade_manager.plugin import get_orders, get_trades, sync_ticker, get_debits, sync_balances, \
    get_credits, \
    make_ledger, get_ticker, get_balances, create_order, sync_orders, cancel_orders, sync_credits, sync_debits, \
    sync_trades, get_order_by_order_id

kraken = Kraken()  # session=ses)
kraken.setup_connections()
SCHEMAS = get_schemas()


def test_commodities():
    map = {'BTC': 'XXBT',
           'ETH': 'XETH',
           'LTC': 'XLTC',
           'USD': 'ZUSD'}
    for good in map:
        assert kraken.unformat_commodity(good) == map[good]
        assert kraken.format_commodity(map[good]) == good


def test_markets():
    map = {'BTC_USD': 'XXBTZUSD',
           'ETH_BTC': 'XETHXXBT',
           'LTC_BTC': 'XLTCXXBT'}
    for good in map:
        assert kraken.unformat_market(good) == map[good]
        assert kraken.format_market(map[good]) == good


class TestPluginRunning(unittest.TestCase):
    def setUp(self):
        start_test_man('kraken')

    def tearDown(self):
        stop_test_man('kraken')

    def test_balance(self):
        sync_balances('kraken')
        countdown = 1000
        total, available = get_balances('kraken', session=kraken.session)
        while countdown > 0 and str(total) == '0':
            countdown -= 1
            total, available = get_balances('kraken', session=kraken.session)
        assert isinstance(total, Balance)
        assert isinstance(available, Balance)
        assert str(total) != 0
        assert str(available) != 0
        for amount in total:
            assert amount >= available.commodity_amount(amount.commodity)

    def test_order_lifecycle(self):
        order = create_order('kraken', 100, 0.01, 'BTC_USD', 'bid', session=kraken.session, expire=time.time()+60)
        assert isinstance(order.id, int)
        assert isinstance(order.price, Amount)
        assert order.price == Amount("100 USD")
        assert order.state == 'pending'
        oorder = get_orders(oid=order.id, session=kraken.session)
        countdown = 1000
        while oorder[0].state == 'pending' and countdown > 0:
            countdown -= 1
            oorder = get_orders(oid=order.id, session=kraken.session)
            if oorder[0].state == 'pending':
                time.sleep(0.01)
                kraken.session.close()
        assert len(oorder) == 1
        assert oorder[0].state == 'open'
        kraken.session.close()
        cancel_orders('kraken', oid=order.id)
        countdown = 1000
        corder = get_orders('kraken', order_id=oorder[0].order_id, session=kraken.session)
        while corder[0].state != 'closed' and countdown > 0:
            countdown -= 1
            corder = get_orders('kraken', order_id=oorder[0].order_id, session=kraken.session)
            if corder[0].state != 'closed':
                time.sleep(0.01)
                kraken.session.close()
        assert len(corder) == 1
        assert corder[0].state == 'closed'

    def test_cancel_order_order_id(self):
        kraken.sync_orders()
        order = create_order('kraken', 100, 0.01, 'BTC_USD', 'bid', session=kraken.session, expire=time.time()+60)
        assert isinstance(order.id, int)
        assert isinstance(order.price, Amount)
        assert order.price == Amount("100 USD")
        assert order.state == 'pending'
        oorder = get_orders(oid=order.id, session=kraken.session)
        countdown = 1000
        while oorder[0].state == 'pending' and countdown > 0:
            countdown -= 1
            oorder = get_orders(oid=order.id, session=kraken.session)
            if oorder[0].state == 'pending':
                time.sleep(0.01)
                kraken.session.close()

        assert len(oorder) == 1
        assert oorder[0].state == 'open'
        print oorder[0].order_id
        kraken.session.close()
        cancel_orders('kraken', order_id=oorder[0].order_id)
        corder = get_order_by_order_id(oorder[0].order_id, 'kraken', kraken.session)
        countdown = 1000
        while (corder is None or corder.state != 'closed') and countdown > 0:
            countdown -= 1
            corder = get_order_by_order_id(oorder[0].order_id, 'kraken', kraken.session)
            if (corder is None or corder.state != 'closed'):
                time.sleep(0.01)
                kraken.session.close()

        assert corder.state == 'closed'

    def test_cancel_order_order_id_no_prefix(self):
        kraken.sync_orders()
        order = create_order('kraken', 100, 0.1, 'BTC_USD', 'bid', session=kraken.session, expire=time.time()+60)
        assert isinstance(order.id, int)
        assert isinstance(order.price, Amount)
        assert order.price == Amount("100 USD")
        assert order.state == 'pending'
        oorder = get_orders(oid=order.id, session=kraken.session)
        countdown = 1000
        while oorder[0].state == 'pending' and countdown > 0:
            countdown -= 1
            oorder = get_orders(oid=order.id, session=kraken.session)
            if oorder[0].state == 'pending':
                time.sleep(0.01)
                kraken.session.close()

        assert len(oorder) == 1
        assert oorder[0].state == 'open'
        print oorder[0].order_id.split("|")[1]
        cancel_orders('kraken', order_id=oorder[0].order_id.split("|")[1])
        #corder = get_orders(oid=order.id, session=kraken.session)
        corder = get_order_by_order_id(oorder[0].order_id, 'kraken', kraken.session)
        countdown = 1000
        while (corder is None or corder.state != 'closed') and countdown > 0:
            countdown -= 1
            #corder = get_orders(oid=order.id, session=kraken.session)
            corder = get_order_by_order_id(oorder[0].order_id, 'kraken', kraken.session)
            if (corder is None or corder.state != 'closed'):
                time.sleep(0.01)
                kraken.session.close()

        assert corder.state == 'closed'

    def test_cancel_orders_by_market(self):
        kraken.sync_orders()
        assert create_order('kraken', 100, 0.1, 'BTC_USD', 'bid', session=kraken.session, expire=time.time()+60) is not None
        last = create_order('kraken', 100, 0.1, 'BTC_USD', 'bid', session=kraken.session, expire=time.time() + 60)
        got = get_order_by_order_id(last.order_id, 'kraken', session=kraken.session)
        countdown = 1000
        while (got is None or got.state != 'open') and countdown > 0:
            countdown -= 1
            got = get_order_by_order_id(last.order_id, 'kraken', session=kraken.session)
            time.sleep(0.01)
            kraken.session.close()
        obids = len(get_orders(side='bid', state='open', session=kraken.session))
        assert obids >= 2
        assert create_order('kraken', 1000, 0.1, 'BTC_USD', 'ask', session=kraken.session, expire=time.time()+60) is not None
        assert create_order('kraken', 1000, 0.1, 'BTC_USD', 'ask', session=kraken.session, expire=time.time()+60) is not None
        last = create_order('kraken', 1000, 0.1, 'ETH_BTC', 'ask', session=kraken.session, expire=time.time()+60)
        got = get_order_by_order_id(last.order_id, 'kraken', session=kraken.session)
        countdown = 1000
        while (got is None or got.state != 'open') and countdown > 0:
            countdown -= 1
            got = get_order_by_order_id(last.order_id, 'kraken', session=kraken.session)
            time.sleep(0.01)
            kraken.session.close()
        oasks = len(get_orders(side='ask', state='open', session=kraken.session))
        assert oasks >= 3
        kraken.session.close()
        cancel_orders('kraken', market='BTC_USD')
        bids = len(get_orders(market='BTC_USD', state='open', session=kraken.session))  # include pending orders? race?
        countdown = 30000
        while bids != 0 and countdown > 0:
            countdown -= 1
            bids = len(get_orders(market='BTC_USD', state='open', session=kraken.session))
            if bids != 0:
                time.sleep(0.01)
                kraken.session.close()
        assert bids == 0
        cancel_orders('kraken', market='ETH_BTC', side='ask')

    def test_cancel_orders_by_side(self):
        kraken.sync_orders()
        assert create_order('kraken', 100, 0.1, 'BTC_USD', 'bid', session=kraken.session, expire=time.time()+60) is not None
        last = create_order('kraken', 100, 0.1, 'BTC_USD', 'bid', session=kraken.session, expire=time.time()+60)
        got = get_order_by_order_id(last.order_id, 'kraken', session=kraken.session)
        countdown = 1000
        while (got is None or got.state != 'open') and countdown > 0:
            countdown -= 1
            got = get_order_by_order_id(last.order_id, 'kraken', session=kraken.session)
            time.sleep(0.01)
            kraken.session.close()
        obids = len(get_orders(side='bid', state='open', session=kraken.session))
        assert obids >= 2
        assert create_order('kraken', 1000, 0.01, 'BTC_USD', 'ask', session=kraken.session, expire=time.time()+60) is not None
        last = create_order('kraken', 1000, 0.01, 'BTC_USD', 'ask', session=kraken.session, expire=time.time()+60)
        got = get_order_by_order_id(last.order_id, 'kraken', session=kraken.session)
        countdown = 1000
        while (got is None or got.state != 'open') and countdown > 0:
            countdown -= 1
            got = get_order_by_order_id(last.order_id, 'kraken', session=kraken.session)
            time.sleep(0.01)
            kraken.session.close()
        oasks = len(get_orders(side='ask', state='open', session=kraken.session))
        assert oasks >= 2
        kraken.session.close()
        cancel_orders('kraken', side='bid')
        bids = len(get_orders(side='bid', state='open', session=kraken.session))  # include pending orders? race?
        countdown = 30000
        while bids != 0 and countdown > 0:
            countdown -= 1
            bids = len(get_orders(side='bid', state='open', session=kraken.session))
            if bids != 0:
                time.sleep(0.01)
                kraken.session.close()

        assert bids == 0
        kraken.session.close()
        asks = len(get_orders(side='ask', state='open', session=kraken.session))
        countdown = 300
        while asks != 0 and countdown > 0:
            countdown -= 1
            asks = len(get_orders(side='ask', state='open', session=kraken.session))
            if asks != 0:
                time.sleep(0.01)
                kraken.session.close()

        assert asks > 0
        assert oasks == asks

    def test_sync_trades(self):
        try:
            kraken.session.delete(kraken.session.query(em.Trade).filter(em.Trade.exchange == 'kraken').first())
            kraken.session.commit()
        except:
            pass
        trades = len(get_trades('kraken', session=kraken.session))
        kraken.session.close()
        sync_trades('kraken', rescan=True)
        newtrades = len(get_trades('kraken', session=kraken.session))
        countdown = 100 * 60 * 60 * 3  # 3 hours
        while newtrades == trades and countdown > 0:
            countdown -= 1
            newtrades = len(get_trades('kraken', session=kraken.session))
            if newtrades == trades:
                time.sleep(0.01)
                kraken.session.close()

        assert newtrades > trades

    def test_sync_credits(self):
        try:
            kraken.session.delete(kraken.session.query(wm.Credit).filter(wm.Credit.reference == 'kraken').first())
            kraken.session.commit()
        except:
            pass
        credits = len(get_credits('kraken', session=kraken.session))
        sync_credits('kraken', rescan=True)
        kraken.session.close()
        newcreds = len(get_credits('kraken', session=kraken.session))
        countdown = 100 * 60 * 30  # half hour
        while newcreds == credits and countdown > 0:
            countdown -= 1
            newcreds = len(get_credits('kraken', session=kraken.session))
            if newcreds == credits:
                time.sleep(0.01)
                kraken.session.close()
        assert newcreds > credits

    def test_sync_debits(self):
        try:
            kraken.session.delete(kraken.session.query(wm.Debit).filter(wm.Debit.reference == 'kraken').first())
            kraken.session.commit()
        except:
            pass
        debits = len(get_debits('kraken', session=kraken.session))
        sync_debits('kraken', rescan=True)
        kraken.session.close()
        newdebs = len(get_debits('kraken', session=kraken.session))
        countdown = 100 * 60 * 30  # half hour
        while newdebs == debits and countdown > 0:
            countdown -= 1
            newdebs = len(get_debits('kraken', session=kraken.session))
            if newdebs == debits:
                time.sleep(0.01)
                kraken.session.close()

        assert newdebs > debits

    def test_ticker(self):
        sync_ticker('kraken', 'BTC_USD')
        ticker = get_ticker('kraken', 'BTC_USD')
        countdown = 1000
        while (ticker is None or len(ticker) == 0) and countdown > 0:
            countdown -= 1
            ticker = get_ticker('kraken', 'BTC_USD')
            if ticker is None:
                time.sleep(0.01)
        tick = json.loads(ticker)
        assert validate(tick, SCHEMAS['Ticker']) is None
