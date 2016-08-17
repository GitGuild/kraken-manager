import time

import datetime

from trade_manager import ses, em, wm
from kraken_manager import Kraken

kraken = Kraken()


def test_save_trades():
    begin = 1374549600
    end = time.time()
    #ses.query(em.Trade)\
    #   .filter(em.Trade.time >= datetime.datetime.strptime('2012-10-29 10:07:00', '%Y-%m-%d %H:%M:%S'))\
    #   .filter(em.Trade.exchange == 'kraken').delete()
    trade = kraken.session.query(em.Trade).filter(em.Trade.exchange == 'kraken').order_by(em.Trade.time.desc()).first()
    if trade is not None:
        kraken.session.delete(trade)
        begin = 'last'
    count = kraken.session.query(em.Trade).count()
    kraken.save_trades(begin=begin, end=end)
    newcount = kraken.session.query(em.Trade).count()
    assert newcount > count


def test_save_credits():
    begin = 1374549600
    end = time.time()
    #kraken.session.query(wm.Credit).filter(wm.Credit.reference == 'kraken').delete()
    credit = kraken.session.query(wm.Credit) \
        .filter(wm.Credit.reference == 'kraken') \
        .order_by(wm.Credit.time.desc()) \
        .first()
    if credit is not None:
        kraken.session.delete(credit)
        begin = 'last'
    count = kraken.session.query(wm.Credit).count()
    kraken.save_credits(begin=begin, end=end)
    newcount = kraken.session.query(wm.Credit).count()
    assert newcount > count


def test_save_debits():
    begin = 1374549600
    end = time.time()
    #kraken.session.query(wm.Debit).filter(wm.Debit.reference == 'kraken').delete()
    debit = kraken.session.query(wm.Debit) \
        .filter(wm.Debit.reference == 'kraken') \
        .order_by(wm.Debit.time.desc()) \
        .first()
    if debit is not None:
        kraken.session.delete(debit)
        begin = 'last'
    count = kraken.session.query(wm.Debit).count()
    kraken.save_debits(begin=begin, end=end)
    time.sleep(0.1)
    newcount = kraken.session.query(wm.Debit).count()
    assert newcount > count


if __name__ == "__main__":
    test_save_trades()
    test_save_credits()
    test_save_debits()
