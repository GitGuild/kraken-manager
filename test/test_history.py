import time

import datetime

from trade_manager import ses, em, wm
from kraken_manager import Kraken

kraken = Kraken()


def test_save_trades():
    begin = 1374549600
    end = time.time()
    ses.query(em.Trade)\
       .filter(em.Trade.time >= datetime.datetime.strptime('2012-10-29 10:07:00', '%Y-%m-%d %H:%M:%S'))\
       .filter(em.Trade.exchange == 'kraken').delete()
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
    kraken.session.query(wm.Credit).filter(wm.Credit.reference == 'kraken').delete()
    credit = kraken.session.query(wm.Credit) \
        .filter(wm.Credit.reference == 'kraken') \
        .order_by(wm.Credit.time.desc()) \
        .first()
    if credit is not None:
        kraken.session.delete(credit)
        begin = 'last'
    kraken.session.add(
        wm.Credit(0.9173, 'QGB3SAA-O6OL4M-F5MXPD', 'BTC', "kraken", "complete", "kraken", "kraken|LIKBE4-OSESN-JYH5OD",
                  kraken.get_manager_user().id,
                  datetime.datetime.utcfromtimestamp(1385831820)))
    kraken.session.add(
        wm.Credit(19.999, 'QGBXI6Z-DYEJTV-VQYXRG', 'BTC', "kraken", "complete", "kraken", "kraken|LRJMRE-3W4EF-LELMEP",
                  kraken.get_manager_user().id,
                  datetime.datetime.strptime('2014-08-19 23:52:52', '%Y-%m-%d %H:%M:%S')))
    kraken.session.add(
        wm.Credit(1, 'QGB3WO2-435JNE-CEQWJJ', 'BTC', "kraken", "complete", "kraken", "kraken|LN3UQA-TDYIY-IHS77C",
                  kraken.get_manager_user().id,
                  datetime.datetime.strptime('2014-10-24 20:53:34', '%Y-%m-%d %H:%M:%S')))
    kraken.session.add(
        wm.Credit(1, 'QGBJG62-HBHGMZ-OP3Y3Z', 'BTC', "kraken", "complete", "kraken", "kraken|LW2L5J-XTST3-X7AIAA",
                  kraken.get_manager_user().id,
                  datetime.datetime.strptime('2014-10-26 20:23:57', '%Y-%m-%d %H:%M:%S')))
    kraken.session.add(
        wm.Credit(1, 'QGBAMRO-PKLECD-3YZPRC', 'BTC', "kraken", "complete", "kraken", "kraken|LWCHPD-QYQGL-FPVYXC",
                  kraken.get_manager_user().id,
                  datetime.datetime.strptime('2014-10-27 03:38:08', '%Y-%m-%d %H:%M:%S')))
    kraken.session.add(
        wm.Credit(2, 'QGBPASU-OTQ5N2-ORYYZ2', 'BTC', "kraken", "complete", "kraken", "kraken|LCFEPU-WHPXC-M3XWM7",
                  kraken.get_manager_user().id,
                  datetime.datetime.strptime('2014-10-27 16:26:46', '%Y-%m-%d %H:%M:%S')))
    kraken.session.commit()
    count = kraken.session.query(wm.Credit).count()
    kraken.save_credits(begin=begin, end=end)
    newcount = kraken.session.query(wm.Credit).count()
    assert newcount > count


def test_save_debits():
    begin = 1374549600
    end = time.time()
    kraken.session.query(wm.Debit).filter(wm.Debit.reference == 'kraken').delete()
    debit = kraken.session.query(wm.Debit) \
        .filter(wm.Debit.reference == 'kraken') \
        .order_by(wm.Debit.time.desc()) \
        .first()
    if debit is not None:
        kraken.session.delete(debit)
        begin = 'last'
    kraken.session.add(
        wm.Debit(0.9168, 0.0005, 'AGBHW6F-LMIYLP-JNIM6P', 'BTC', "kraken", "complete", "kraken",
                 "kraken|LX6EEH-THYRM-FJXYDE",
                 kraken.get_manager_user().id,
                 datetime.datetime.strptime('2014-06-30 17:40:56', '%Y-%m-%d %H:%M:%S')))
    kraken.session.add(
        wm.Debit(19.98894, 0.0005, 'AGBUXLG-EONZUK-WW24BW', 'BTC', "kraken", "complete", "kraken",
                 "kraken|L3O52M-SDEOD-RLBXJC",
                 kraken.get_manager_user().id,
                 datetime.datetime.strptime('2014-10-03 23:13:07', '%Y-%m-%d %H:%M:%S')))
    kraken.session.commit()
    count = kraken.session.query(wm.Debit).count()
    kraken.save_debits(begin=begin, end=end)
    time.sleep(0.1)
    newcount = kraken.session.query(wm.Debit).count()
    assert newcount > count


if __name__ == "__main__":
    test_save_trades()
    test_save_credits()
    test_save_debits()
