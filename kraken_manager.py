"""
Plugin for managing a Kraken account.
This module can be imported by trade_manager and used like a plugin.
"""
import base64
import datetime
import hashlib
import hmac
import json
import time
import urllib
from ledger import Amount, Balance
import requests
from requests import Timeout
from requests.exceptions import ReadTimeout
from requests.packages.urllib3.connection import ConnectionError

from sqlalchemy_models import jsonify2
from trade_manager import em, wm
from trade_manager.plugin import ExchangePluginBase, get_order_by_order_id, submit_order

baseUrl = 'https://api.kraken.com'
REQ_TIMEOUT = 10  # seconds

FIAT_CURRENCIES = ['USD', 'EUR', 'GBP']


class Kraken(ExchangePluginBase):
    NAME = 'kraken'
    _user = None

    def submit_private_request(self, method, params=None, retry=0):
        """Submit request to Kraken"""
        if not params:
            params = {}
        path = '/0/private/%s' % method

        params['nonce'] = int(time.time() * 1000)
        data = urllib.urlencode(params)
        message = path + hashlib.sha256(str(params['nonce']) + data).digest()
        sign = base64.b64encode(hmac.new(base64.b64decode(self.secret),
                                         message, hashlib.sha512).digest())
        headers = {
            'API-Key': self.key,
            'API-Sign': sign
        }
        try:
            rawresp = requests.post(baseUrl + path, data=data, headers=headers, timeout=REQ_TIMEOUT)
            response = rawresp.text
        except (ConnectionError, ReadTimeout, Timeout) as e:
            self.logger.exception('%s %s while sending %r to kraken %s' % (type(e), e, params, path))
            if retry < 3:
                return self.submit_private_request(method, params=params, retry=retry + 1)
        if rawresp.status_code == 502 or rawresp.status_code == 520:
            self.logger.exception('%s error while sending %r to kraken %s' % (rawresp.status_code, params, path))
            if retry < 3:
                return self.submit_private_request(method, params=params, retry=retry + 1)
        try:
            jresp = json.loads(response)
        except ValueError as e:
            self.logger.exception('%s %s while sending %r to kraken %s, response %s' % (type(e), e, params, path, response))
            return
        if "Invalid nonce" in response and retry < 3:
            return self.submit_private_request(method, params=params, retry=retry + 1)
        else:
            return jresp

    @classmethod
    def submit_public_request(cls, method, params=None):
        path = '/0/public/%s' % method
        data = urllib.urlencode(params)
        return json.loads(requests.get(baseUrl + path + "?" + data, timeout=REQ_TIMEOUT).text)

    @classmethod
    def format_market(cls, market):
        """
        The default market symbol is an uppercase string consisting of the base commodity
        on the left and the quote commodity on the right, separated by an underscore.

        If the data provided by the exchange does not match the default
        implementation, then this method must be re-implemented.

        :return: a market formated according to what bitcoin_exchanges expects.
        """
        middle = int(len(market) / 2)
        half1 = cls.format_commodity(market[:middle].strip("_"))
        half2 = cls.format_commodity(market[middle:].strip("_"))
        return "%s_%s" % (half1, half2)

    @classmethod
    def unformat_market(cls, market):
        """
        Reverse format a market to the format recognized by the exchange.
        If the data provided by the exchange does not match the default
        implementation, then this method must be re-implemented.

        :return: a market formated according to what kraken expects.
        """
        if "_" in market or len(market) == 8:
            middle = int(len(market) / 2)
            half1 = cls.unformat_commodity(market[:middle].strip("_"))
            half2 = cls.unformat_commodity(market[middle:].strip("_"))
            market = half1 + half2
        return market

    @classmethod
    def format_commodity(cls, c):
        """
        The default commodity symbol is an uppercase string of 3 or 4 letters.

        If the data provided by the exchange does not match the default
        implementation, then this method must be re-implemented.
        """
        if len(c) > 3 and c[0] in "XZ":
            c = c[1:]
        if c == "XBT":
            return "BTC"
        return c

    @classmethod
    def unformat_commodity(cls, c):
        """
        Reverse format a commodity to the format recognized by the exchange.
        If the data provided by the exchange does not match the default
        implementation, then this method must be re-implemented.
        """
        c = c.strip('_')
        if len(c) == 4 and c[0] in "XY":
            return c
        elif len(c) == 3 and c[0] not in "XY":
            if c == 'BTC':
                c = 'XBT'
            if c in FIAT_CURRENCIES:
                return 'Z' + c
            else:
                return 'X' + c
        return c

    @classmethod
    def sync_book(cls, market=None):
        pass

    def sync_ticker(self, market='BTC_USD'):
        pair = self.unformat_market(market)
        full_ticker = self.submit_public_request('Ticker', {'pair': pair})
        ticker = full_ticker['result'][pair]
        tick = em.Ticker(float(ticker['b'][0]),
                         float(ticker['a'][0]),
                         float(ticker['h'][1]),
                         float(ticker['l'][1]),
                         float(ticker['v'][1]),
                         float(ticker['c'][0]),
                         market, 'kraken')
        jtick = jsonify2(tick, 'Ticker')
        self.red.set('kraken_%s_ticker' % market, jtick)
        return tick

    def sync_balances(self):
        tbal = self.submit_private_request('Balance')
        if 'result' in tbal:
            total = Balance()
            for cur in tbal['result']:
                commodity = self.format_commodity(cur)
                amount = Amount("{0} {1}".format(tbal['result'][cur], commodity))
                total = total + amount
        else:
            total = Balance()
        # self.logger.debug("total balance: %s" % total)
        available = Balance(total)
        oorders = self.get_open_orders()
        for o in oorders:
            o.load_commodities()
            if o.side == 'bid':
                available = available - o.price * o.amount.number()
            else:
                available = available - o.amount

        # self.logger.debug("available balance: %s" % available)
        bals = {}
        for amount in total:
            comm = str(amount.commodity)
            bals[comm] = self.session.query(wm.Balance).filter(wm.Balance.user_id == self.manager_user.id) \
                .filter(wm.Balance.currency == comm).one_or_none()
            if not bals[comm]:
                bals[comm] = wm.Balance(amount, available.commodity_amount(amount.commodity), comm, "",
                                        self.manager_user.id)
                self.session.add(bals[comm])
            else:
                bals[comm].load_commodities()
                bals[comm].total = amount
                bals[comm].available = available.commodity_amount(amount.commodity)
        try:
            self.session.commit()
        except Exception as e:
            self.logger.exception(e)
            self.session.rollback()
            self.session.flush()

    def sync_orders(self):
        orders = self.submit_private_request('ClosedOrders', {'trades': 'False'})
        if 'result' in orders and 'closed' in orders['result']:
            rawos = orders['result']['closed']
            for id, o in rawos.iteritems():
                side = 'ask' if o['descr']['type'] == 'sell' else 'bid'
                base = self.base_commodity(o['descr']['pair'])
                quote = self.quote_commodity(o['descr']['pair'])
                amount = Amount("%s %s" % (o['vol'], base)) - Amount("%s %s" % (o['vol_exec'], base))
                lo = get_order_by_order_id(id, 'kraken', session=self.session)
                # TODO update state and exec amount
                if lo is None:
                    lo = em.LimitOrder(Amount("%s %s" % (o['price'], quote)), amount,
                                       self.format_market(o['descr']['pair']), side, 'kraken',
                                       state='closed', order_id='kraken|%s' % id)
                    self.session.add(lo)
            try:
                self.session.commit()
            except Exception as e:
                self.logger.exception(e)
                self.session.rollback()
                self.session.flush()
        return orders

    @classmethod
    def get_order_book(cls, market='BTC_USD'):
        market = cls.unformat_market(market)
        book = cls.submit_public_request('Depth', {'pair': market})
        return book['result'][market]

    # private methods
    def cancel_order(self, oid=None, order_id=None, order=None):
        if order is None and oid is not None:
            order = self.session.query(em.LimitOrder).filter(em.LimitOrder.id == oid).first()
        elif order is None and order_id is not None:
            order = self.session.query(em.LimitOrder).filter(em.LimitOrder.order_id == order_id).first()
        elif order is None:
            return
        resp = self.submit_private_request('CancelOrder', {'txid': order.order_id.split("|")[1]})
        if resp and 'result' in resp and 'count' in resp['result'] and resp['result']['count'] > 0:
            order.state = 'closed'
            order.order_id = order.order_id.replace('tmp', 'kraken')
            try:
                self.session.commit()
            except Exception as e:
                self.logger.exception(e)
                self.session.rollback()
                self.session.flush()

    def cancel_orders(self, oid=None, order_id=None, market=None, side=None, price=None):
        if oid is not None or order_id is not None:
            order = self.session.query(em.LimitOrder)
            if oid is not None:
                order = order.filter(em.LimitOrder.id == oid).first()
            elif order_id is not None:
                order_id = order_id if "|" not in order_id else "kraken|%s" % order_id.split("|")[1]
                order = get_order_by_order_id(order_id, 'kraken', session=self.session)
            self.cancel_order(order=order)
        else:
            orders = self.get_open_orders(market=market)
            for o in orders:
                if market is not None and market != o.market:
                    continue
                if side is not None and side != o.side:
                    continue
                if price is not None:
                    if o.side == 'bid' and o.price < price:
                        continue
                    elif o.side == 'ask' and o.price > price:
                        continue
                self.cancel_order(order=o)

    def create_order(self, oid, expire=None):
        order = self.session.query(em.LimitOrder).filter(em.LimitOrder.id == oid).first()
        if not order:
            self.logger.warning("unable to find order %s" % oid)
            if expire is not None and expire < time.time():
                submit_order('kraken', oid, expire=expire)  # back of the line!
            return
        market = self.unformat_market(order.market)
        amount = str(order.amount.number()) if isinstance(order.amount, Amount) else str(order.amount)
        price = str(order.price.number()) if isinstance(order.price, Amount) else str(order.price)
        side = 'buy' if order.side == 'bid' else 'sell'
        options = {'type': side, 'volume': amount, 'price': price, 'pair': market, 'ordertype': 'limit'}
        resp = None
        try:
            resp = self.submit_private_request('AddOrder', options)
        except Exception as e:
            self.logger.exception(e)
        if resp is None or 'error' in resp and len(resp['error']) > 0:
            self.logger.warning('kraken unable to create order %r for reason %r' % (options, resp))
            # Do nothing. The order can stay locally "pending" and be retried, if desired.
        elif 'result' in resp and 'txid' in resp['result'] and len(resp['result']['txid']) > 0:
            order.order_id = 'kraken|%s' % resp['result']['txid'][0]
            order.state = 'open'
            self.logger.debug("submitted order %s" % order)
            try:
                self.session.commit()
            except Exception as e:
                self.logger.exception(e)
                self.session.rollback()
                self.session.flush()
            return order

    def get_open_orders(self, market=None):
        oorders = self.submit_private_request('OpenOrders', {'trades': 'True'})
        orders = []

        if 'result' in oorders and 'open' in oorders['result']:
            rawos = oorders['result']['open']
            for id, o in rawos.iteritems():
                side = 'ask' if o['descr']['type'] == 'sell' else 'bid'
                pair = self.format_market(o['descr']['pair'])
                base = self.base_commodity(pair)
                amount = Amount("%s %s" % (o['vol'], base)) - Amount("%s %s" % (o['vol_exec'], base))
                quote = self.quote_commodity(pair)
                if market is None or pair == self.format_market(market):
                    try:
                        lo = get_order_by_order_id(id, 'kraken', session=self.session)
                    except Exception as e:
                        self.logger.exception(e)
                    if lo is None:
                        lo = em.LimitOrder(Amount("%s %s" % (o['descr']['price'], quote)), amount, pair, side,
                                           self.NAME, str(id), exec_amount=Amount("0 %s" % base), state='open')
                        self.session.add(lo)
                    else:
                        lo.state = 'open'
                    orders.append(lo)
        try:
            self.session.commit()
        except Exception as e:
            self.logger.exception(e)
            self.session.rollback()
            self.session.flush()
        return orders

    def get_trades_history(self, begin=None, tend=None, market=None, offset=None):
        # TODO market is ignored... filter for market
        params = {}
        if begin is not None:
            params['start'] = str(begin)
        if tend is not None:
            params['end'] = str(tend)
        if offset is not None:
            params['ofs'] = str(offset)
        return self.submit_private_request('TradesHistory', params)

    def sync_trades(self, market=None, rescan=False):
        offset = 0
        lastoffset = -1
        lastsleep = 4
        trades = None
        changed = False
        while offset != lastoffset:
            self.logger.debug("begin offset\t%s\nlastoffset\t%s" % (offset, lastoffset))
            try:
                trades = self.get_trades_history(market=market, offset=offset)
            except (IOError, ValueError) as e:
                if "ReadTimeout" in str(e):
                    lastsleep *= 2
                    time.sleep(lastsleep)
                    continue
                return
            if not trades or 'result' not in trades or trades['result']['count'] == 0:
                self.logger.debug("; non-interesting trades %s" % trades)
                if "error" in trades and len(trades['error']) > 0 and \
                        "Rate limit exceeded" in trades['error'][0]:
                    lastsleep *= 1.5
                    time.sleep(lastsleep)
                    continue
                elif "error" in trades and len(trades['error']) > 0 and \
                        "Invalid nonce" in trades['error'][0]:
                    time.sleep(30)
                    continue
                return
            if lastsleep > 1:
                lastsleep *= 0.975
            lastoffset = offset
            for tid in trades['result']['trades']:
                lastoffset = offset
                if rescan:
                    offset += 1
                found = self.session.query(em.Trade) \
                    .filter(em.Trade.trade_id == 'kraken|%s' % tid) \
                    .count()
                if found != 0:
                    self.logger.debug("%s already known" % tid)
                    continue
                row = trades['result']['trades'][tid]
                dtime = datetime.datetime.fromtimestamp(float(row['time']))
                market = self.format_market(row['pair'])
                price = float(row['price'])
                amount = float(row['vol'])
                fee = float(row['fee'])
                side = row['type']
                trade = em.Trade(tid, 'kraken', market, side, amount, price, fee, 'quote', dtime)
                self.session.add(trade)
                changed = True
                self.logger.debug("end offset\t%s\nlastoffset\t%s" % (offset, lastoffset))
        if changed:
            self.session.commit()

    def get_ledgers(self, ltype='all', begin=None, tend=None, ofs=None):
        params = {'type': ltype}
        if begin is not None:
            params['start'] = str(begin)
        if tend is not None:
            params['end'] = str(tend)
        if ofs is not None:
            params['ofs'] = str(ofs)
        return self.submit_private_request('Ledgers', params)

    def sync_credits(self, rescan=False):
        offset = 0
        lastoffset = -1
        lastsleep = 2
        changed = False
        while offset != lastoffset:
            ledgers = None
            self.logger.debug("begin offset\t%s\nlastoffset\t%s" % (offset, lastoffset))
            try:
                ledgers = self.get_ledgers(ofs=offset, ltype='deposit')
            except (IOError, ValueError) as e:
                self.logger.exception(e)
                if "ReadTimeout" in str(e):
                    lastsleep *= 2
                    time.sleep(lastsleep)
                    continue
                return
            if not ledgers or 'result' not in ledgers or ledgers['result']['count'] == 0:
                self.logger.warning(ledgers)
                if "error" in ledgers and len(ledgers['error']) > 0 and \
                                "Rate limit exceeded" in ledgers['error'][0]:
                    lastsleep *= 1.5
                    time.sleep(lastsleep)
                    continue
                elif "error" in ledgers and len(ledgers['error']) > 0 and \
                                "Invalid nonce" in ledgers['error'][0]:
                    time.sleep(30)
                    continue
                return
            if lastsleep > 1:
                lastsleep *= 0.95
            lastoffset = offset
            for bid in ledgers['result']['ledger']:
                row = ledgers['result']['ledger'][bid]
                lastoffset = offset
                if rescan:
                    offset += 1
                found = self.session.query(wm.Credit) \
                    .filter(wm.Credit.ref_id == 'kraken|%s' % bid) \
                    .count()
                if found != 0:
                    self.logger.debug("%s already known" % bid)
                    continue
                dtime = datetime.datetime.fromtimestamp(float(row['time']))
                asset = self.format_commodity(row['asset'])
                amount = Amount("%s %s" % (row['amount'], asset))
                refid = row['refid']
                cred = wm.Credit(amount, refid, asset, "kraken", "complete", "kraken", "kraken|%s" % bid,
                                 self.manager_user.id, dtime)
                self.session.add(cred)
                changed = True
                self.logger.debug("end offset\t%s\nlastoffset\t%s" % (offset, lastoffset))
        if changed:
            self.session.commit()

    def sync_debits(self, rescan=False):
        offset = 0
        lastoffset = -1
        lastsleep = 2
        ledgers = None
        changed = True
        while offset != lastoffset:
            try:
                ledgers = self.get_ledgers(ofs=offset, ltype='withdrawal')
            except (IOError, ValueError) as e:
                if "ReadTimeout" in str(e):
                    lastsleep *= 2
                    time.sleep(lastsleep)
                    continue
            if not ledgers or 'result' not in ledgers or ledgers['result']['count'] == 0:
                self.logger.debug("; non-interesting ledgers %s" % ledgers)
                if "error" in ledgers and len(ledgers['error']) > 0 and \
                        "Rate limit exceeded" in ledgers['error'][0]:
                    lastsleep *= 2
                    time.sleep(lastsleep)
                elif "error" in ledgers and len(ledgers['error']) > 0 and \
                        "Invalid nonce" in ledgers['error'][0]:
                    time.sleep(30)
                continue
            if lastsleep > 1:
                lastsleep -= 1
            lastoffset = offset
            for bid in ledgers['result']['ledger']:
                lastoffset = offset
                if rescan:
                    offset += 1
                found = self.session.query(wm.Debit) \
                    .filter(wm.Debit.ref_id == 'kraken|%s' % bid) \
                    .count()
                if found != 0:
                    self.logger.debug("; %s already known" % bid)
                    continue
                row = ledgers['result']['ledger'][bid]
                dtime = datetime.datetime.fromtimestamp(float(row['time']))
                asset = self.format_commodity(row['asset'])
                amount = Amount("%s %s" % (row['amount'], asset))
                fee = Amount("%s %s" % (row['fee'], asset))
                refid = row['refid']
                self.session.add(wm.Debit(amount, fee, refid, asset, "kraken", "complete", "kraken", "kraken|%s" % bid,
                                          self.manager_user.id, dtime))
                changed = True
                self.logger.debug("end offset\t%s\nlastoffset\t%s" % (offset, lastoffset))
        if changed:
            self.session.commit()


def main():
    kraken = Kraken()
    kraken.run()


if __name__ == "__main__":
    main()
