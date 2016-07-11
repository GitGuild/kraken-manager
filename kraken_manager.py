"""
Plugin for managing a Kraken account.
This module can be imported by trade_manager and used like a plugin.
"""
import base64
import datetime
import hmac
import json
import time
import urllib
from ledger import Amount, Balance

import hashlib
import requests
from requests.exceptions import Timeout, ConnectionError

from trade_manager import CFG, em, wm, ses, ExchangeError, make_ledger
from trade_manager.plugin import InternalExchangePlugin

NAME = 'kraken'
KEY = CFG.get('kraken', 'key')

baseUrl = 'https://api.kraken.com'
REQ_TIMEOUT = 10  # seconds
LEDGER_PERIOD = 604800 * 12# 3 month


def unadjust_currency(c):
    if len(c) > 3 and c[0] in "XZ":
        c = c[1:]
    if c == "XBT":
        c = "BTC"
    return c


class Kraken(InternalExchangePlugin):
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
            response = json.loads(requests.post(baseUrl + path, data=data, headers=headers,
                                                timeout=REQ_TIMEOUT).text)
        except (ConnectionError, Timeout, ValueError) as e:
            raise ExchangeError('kraken', '%s %s while sending %r to %s' % (type(e), e, params, path))
        if "Invalid nonce" in response and retry < 3:
            return self.submit_private_request(method, params=params, retry=retry + 1)
        else:
            return response

    @classmethod
    def submit_public_request(cls, method, params=None):
        path = '/0/public/%s' % method
        data = urllib.urlencode(params)
        try:
            return json.loads(requests.get(baseUrl + path + "?" + data, timeout=REQ_TIMEOUT).text)
        except (ConnectionError, Timeout, ValueError) as e:
            raise ExchangeError('btce', '%s %s while sending %r to %s' % (type(e), e, params, path))

    @classmethod
    def format_pair(cls, pair):
        """
        The default pair symbol is an uppercase string consisting of the base currency 
        on the left and the quote currency on the right, separated by an underscore.
        
        If the data provided by the exchange does not match the default
        implementation, then this method must be re-implemented.

        :return: a pair formated according to what bitcoin_exchanges expects.
        """
        middle = int(len(pair) / 2)
        half1 = unadjust_currency(pair[:middle].strip("_"))
        half2 = unadjust_currency(pair[middle:].strip("_"))
        return "%s_%s" % (half1, half2)

    @classmethod
    def unformat_pair(cls, pair):
        """
        Reverse format a pair to the format recognized by the exchange.
        If the data provided by the exchange does not match the default
        implementation, then this method must be re-implemented.

        :return: a pair formated according to what kraken expects.
        """
        if pair[0] != 'X':
            middle = int(len(pair) / 2)
            half1 = pair[:middle].strip("_")
            half2 = pair[middle:].strip("_")
            #half1, half2 = pair.split("_")
            if half1 == 'BTC':
                half1 = 'XBT'
            if half2 in ['BTC', 'XBT']:
                half2 = 'XXBT'
            else:
                half2 = 'Z%s' % half2
            pair = 'X%s%s' % (half1, half2)
        return pair

    @classmethod
    def get_ticker(cls, pair='BTC_USD'):
        sp = pair.split("_")
        base = sp[0]
        quote = sp[1]
        pair = cls.unformat_pair(pair)
        fullticker = cls.submit_public_request('Ticker', {'pair': pair})
        ticker = fullticker['result'][pair]
        return create_ticker(ask=ticker['a'][0], bid=ticker['b'][0],
                             timestamp=time.time(), volume=ticker['v'][1],
                             last=ticker['c'][0], high=ticker['h'][1],
                             low=ticker['l'][1], currency=quote, vcurrency=base)

    @classmethod
    def get_order_book(cls, pair='BTC_USD'):
        pair = cls.unformat_pair(pair)
        book = cls.submit_public_request('Depth', {'pair': pair})
        return book['result'][pair]

    @classmethod
    def get_trades(cls, pair):
        pair = cls.unformat_pair(pair)
        return cls.submit_public_request('Trades', {'pair': pair})

    @classmethod
    def get_spread(cls, pair='BTC_USD'):
        pair = cls.unformat_pair(pair)
        return cls.submit_public_request('Spread', {'pair': pair})

    # private methods
    def cancel_order(self, oid, pair=None):
        # TODO check pair?
        if pair is not None:
            pair = self.unformat_pair(pair)
        resp = self.submit_private_request('CancelOrder', {'txid': oid})
        if resp and 'result' in resp and 'count' in resp['result'] and resp['result']['count'] > 0:
            return True
        return False

    def cancel_orders(self, pair=None, **kwargs):
        orders = self.get_open_orders()
        success = True
        for o in orders:
            # TODO check pair
            resp = self.cancel_order(o.order_id)
            if not resp:
                success = False
        return success

    def create_order(self, amount, price, otype, pair='BTC_USD', **kwargs):
        if BLOCK_ORDERS:
            return "order blocked"
        pair = self.unformat_pair(pair)
        otype = 'buy' if otype == 'bid' else 'sell'
        if isinstance(amount, Amount):
            amount = str(amount.number())
        elif not isinstance(amount, str):
            amount = str(amount)
        if isinstance(price, Amount):
            price = str(price.number())
        elif not isinstance(price, str):
            price = str(price)
        options = {'type': otype, 'volume': amount, 'price': price, 'pair': pair, 'ordertype': 'limit'}
        options.update(kwargs)
        resp = self.submit_private_request('AddOrder', options)
        if 'error' in resp and len(resp['error']) > 0:
            raise ExchangeError('kraken', 'unable to create order %r for reason %r' % (options, resp['error']))
        elif 'result' in resp and 'txid' in resp['result'] and len(resp['result']['txid']) > 0:
            return str(resp['result']['txid'][0])

    def get_closed_orders(self):
        return self.submit_private_request('ClosedOrders', {'trades': 'True'})

    def get_balance(self, btype='total'):
        tbal = self.get_total_balance()
        if 'result' in tbal:
            total = Balance()
            for cur in tbal['result']:
                if cur == 'XXBT':
                    total += Amount("{0} {1}".format(tbal['result']['XXBT'], 'BTC'))
                elif cur == 'ZEUR':
                    total += Amount("{0} {1}".format(tbal['result']['ZEUR'], 'EUR'))
                elif cur == 'ZUSD':
                    total += Amount("{0} {1}".format(tbal['result']['ZUSD'], 'USD'))
                elif cur == 'XETH':
                    total += Amount("{0} {1}".format(tbal['result']['XETH'], 'ETH'))
                elif cur == 'XLTC':
                    total += Amount("{0} {1}".format(tbal['result']['XLTC'], 'LTC'))
        else:
            total = Balance()

        if btype == 'total':
            return total

        available = Balance(total)
        oorders = self.get_open_orders()
        for o in oorders:
            if o.side == 'bid':
                available -= o.price * o.amount.number()
            else:
                available -= o.amount

        if btype == 'available':
            return available
        else:
            return total, available

    def get_balance_by_asset(self):
        return self.submit_private_request('Balance')

    def get_total_balance(self):
        return self.submit_private_request('Balance')

    def get_open_orders(self, pair=None):
        oorders = self.submit_private_request('OpenOrders', {'trades': 'True'})
        orders = []
        if pair is not None:
             pair = self.unformat_pair(pair)
        if 'result' in oorders and 'open' in oorders['result']:
            rawos = oorders['result']['open']
            for id, o in rawos.iteritems():
                side = 'ask' if o['descr']['type'] == 'sell' else 'bid'
                base = unadjust_currency(o['descr']['pair'][:3])
                amount = Amount("%s %s" % (o['vol'], base)) - Amount("%s %s" % (o['vol_exec'], base))
                quote = unadjust_currency(o['descr']['pair'][3:])
                if pair is None or self.unformat_pair(o['descr']['pair']) == pair:
                    orders.append(MyOrder(Amount("%s %s" % (o['descr']['price'], quote)), amount, side, self.NAME, str(id)))
        return orders

    def get_deposit_address(self, method='Bitcoin', asset='BTC'):
        addys = self.submit_private_request('DepositAddresses', 
                {'asset': asset, 'method':method})
        if len(addys['error']) > 0:
            raise ExchangeError('kraken', addys['error'])
        for addy in addys['result']:
            if int(addy['expiretm']) < time.time() + 1440:
                return str(addy['address'])
        raise ExchangeError('kraken', "unable to get deposit address")

    def get_trades_history(self, begin='last', end=None, pair=None, offset=None):
        # TODO pair is ignored... filter for pair
        params = {'trades': 'True'}
        if begin == 'last':
            last = ses.query(em.Trade)\
                        .filter(em.Trade.exchange=='kraken')\
                        .order_by(em.Trade.time.desc())\
                        .first()
            params['start'] = str(time.mktime(last.time.timetuple()))
        elif begin is not None:
            params['start'] = str(begin)
        if end is not None:
            params['end'] = str(end)
        if offset is not None:
            params['ofs'] = str(offset)
        return self.submit_private_request('TradesHistory', params)

    def save_trades(self, begin='last', end=None, pair=None):
        offset = 0
        lastoffset = -1
        lastsleep = 4
        trades = None
        while offset != lastoffset:
            try:
                trades = self.get_trades_history(begin, end, pair, offset)
            except ExchangeError as e:
                if "ReadTimeout" in str(e):
                    lastsleep *= 2
                    time.sleep(lastsleep)
                    continue
            if not trades or 'result' not in trades or trades['result']['count'] == 0:
                print "; non-interesting trades %s" % trades
                if "error" in trades and len(trades['error']) > 0 and \
                        "Rate limit exceeded" in trades['error'][0]:
                    lastsleep *= 1.5
                    time.sleep(lastsleep)
                elif "error" in trades and len(trades['error']) > 0 and \
                        "Invalid nonce" in trades['error'][0]:
                    time.sleep(30)
                continue
            if lastsleep > 1:
                lastsleep *= 0.975
            lastoffset = offset
            for tid in trades['result']['trades']:
                lastoffset = offset
                offset += 1
                found = ses.query(em.Trade)\
                        .filter(em.Trade.trade_id=='kraken|%s' % tid)\
                        .count()
                if found != 0:
                    print "; %s already known" % tid
                    continue
                row = trades['result']['trades'][tid]
                dtime = datetime.datetime.fromtimestamp(float(row['time']))
                market = self.format_pair(row['pair'])
                price = float(row['price'])
                amount = float(row['vol'])
                fee = float(row['fee'])
                side = row['type']
                ses.add(em.Trade(tid, 'kraken', market, side,
                        amount, price, fee, 'quote', dtime))
            ses.commit()

    def get_ledgers(self, ltype='all', begin=None, end=None, ofs=None):
        params = {'type': ltype}
        if begin == 'last':
            if ltype == 'deposit' or ltype == 'all':
                lastcred = ses.query(wm.Credit)\
                            .filter(wm.Credit.reference=='kraken')\
                            .order_by(wm.Credit.time.desc()).first()
                if lastcred:
                    params['start'] = str(int(time.mktime(lastcred.time.timetuple())) - 1)
            if ltype == 'withdrawal' or ltype == 'all':
                lastdeb = ses.query(wm.Debit)\
                            .filter(wm.Debit.reference=='kraken')\
                            .order_by(wm.Debit.time.desc())\
                            .first()
                if lastdeb is not None:
                    timmy = int(time.mktime(lastdeb.time.timetuple()))
                    if 'start' not in params or int(params['start']) > timmy:
                        params['start'] = str(timmy - 1)                
        elif begin is not None:
            params['start'] = str(begin)
        if end is not None:
            params['end'] = str(end)
        if ofs is not None:
            params['ofs'] = str(ofs)
        return self.submit_private_request('Ledgers', params)

    def save_credits(self, begin='last', end=None):
        offset = 0
        lastoffset = -1
        lastsleep = 2
        ledgers = None
        while offset != lastoffset:
            try:
                ledgers = self.get_ledgers(ofs=offset, begin=begin,
                                           end=end, ltype='deposit')
            except ExchangeError as e:
                if "ReadTimeout" in str(e):
                    lastsleep *= 2
                    time.sleep(lastsleep)
                    return
            if not ledgers or 'result' not in ledgers or ledgers['result']['count'] == 0:
                print "non-interesting ledgers %s" % ledgers
                if "error" in ledgers and len(ledgers['error']) > 0 and \
                        "Rate limit exceeded" in ledgers['error'][0]:
                    lastsleep *= 1.5
                    time.sleep(lastsleep)
                elif "error" in ledgers and len(ledgers['error']) > 0 and \
                        "Invalid nonce" in ledgers['error'][0]:
                    time.sleep(30)
                return
            if lastsleep > 1:
                lastsleep *= 0.95
            lastoffset = offset
            for bid in ledgers['result']['ledger']:
                row = ledgers['result']['ledger'][bid]
                lastoffset = offset
                offset += 1
                found = ses.query(wm.Credit)\
                        .filter(wm.Credit.ref_id=='kraken|%s' % bid)\
                        .count()
                if found != 0:
                    print "%s already known" % bid
                    continue
                dtime = datetime.datetime.fromtimestamp(float(row['time']))
                asset = unadjust_currency(row['asset'])
                amount = Amount("%s %s" % (row['amount'], asset))
                refid = row['refid']
                ses.add(wm.Credit(amount, refid, asset, "kraken", "complete", "kraken", "kraken|%s" % bid, self.get_manager_user().id, dtime))
            ses.commit()

    def save_debits(self, begin='last', end=None):
        offset = 0
        lastoffset = -1
        lastsleep = 2
        ledgers = None
        while offset != lastoffset:
            try:
                ledgers = self.get_ledgers(ofs=offset, begin=begin,
                                           end=end, ltype='withdrawal')
            except ExchangeError as e:
                if "ReadTimeout" in str(e):
                    lastsleep *= 2
                    time.sleep(lastsleep)
                    return
            if not ledgers or 'result' not in ledgers or ledgers['result']['count'] == 0:
                print "; non-interesting ledgers %s" % ledgers
                if "error" in ledgers and len(ledgers['error']) > 0 and \
                        "Rate limit exceeded" in ledgers['error'][0]:
                    lastsleep *= 2
                    time.sleep(lastsleep)
                elif "error" in ledgers and len(ledgers['error']) > 0 and \
                        "Invalid nonce" in ledgers['error'][0]:
                    time.sleep(30)
                return
            if lastsleep > 1:
                lastsleep -= 1
            lastoffset = offset
            for bid in ledgers['result']['ledger']:
                lastoffset = offset
                offset += 1
                found = ses.query(wm.Debit)\
                        .filter(wm.Debit.ref_id=='kraken|%s' % bid)\
                        .count()
                if found != 0:
                    print "; %s already known" % bid
                    continue
                row = ledgers['result']['ledger'][bid]
                dtime = datetime.datetime.fromtimestamp(float(row['time']))
                asset = unadjust_currency(row['asset'])
                amount = Amount("%s %s" % (row['amount'], asset))
                fee = Amount("%s %s" % (row['fee'], asset))
                refid = row['refid']
                ses.add(wm.Debit(amount, fee, refid, asset, "kraken", "complete", "kraken", "kraken|%s" % bid, self.get_manager_user().id, dtime))
            ses.commit()

    """
    The remaining methods are unused, but supported by Kraken.
    """
    @classmethod
    def get_time(cls):
        return cls.submit_public_request('Time')

    @classmethod
    def get_info(cls):
        return cls.submit_public_request('Assets')

    @classmethod
    def get_pairs(cls):
        return cls.submit_public_request('AssetPairs')

    @classmethod
    def get_ohlc(cls, pair):
        return cls.submit_public_request(method='OHLC', params={'pair': pair})


if __name__ == "__main__":
    kraken = Kraken()
    kraken.save_trades()
    kraken.save_credits()
    kraken.save_debits()
    ledger = make_ledger(exchange='kraken')
    print ledger
