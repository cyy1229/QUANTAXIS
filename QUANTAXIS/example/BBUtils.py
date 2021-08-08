from pymongo import DESCENDING, ASCENDING

from QUANTAXIS import DATABASE
from datetime import datetime

import json
import numpy as np
import pandas as pd

def query_last_trade_date(dt):
    '''查询上一个交易日，如果传入的当日是交易日，则算传入的日期'''
    return (list(DATABASE.trade_date.find({'cal_date': {'$lte': dt}}).sort('cal_date', DESCENDING).limit(1)))[0][
        'cal_date']


def query_next_trade_date(dt):
    '''查询传入参数日期的下一个交易日，如果传入的当日是交易日，不算'''
    return (list(DATABASE.trade_date.find({'cal_date': {'$gt': dt}}).sort('cal_date', ASCENDING).limit(1)))[0]['cal_date']


def query_next_all_trade_date(dt):
    '''查询传入参数日期的下一个交易日，如果传入的当日是交易日，不算'''
    return [item['cal_date'] for item in
            DATABASE.trade_date.find({'cal_date': {'$gt': dt}}, {'cal_date': 1, "_id": False}).sort('cal_date',
                                                                                                    ASCENDING)]


def query_before_all_trade_date(dt):
    '''查询传入参数日期的上所有交易日，如果传入的当日是交易日也算上'''
    return [item['cal_date'] for item in
            DATABASE.trade_date.find({'cal_date': {'$lte': dt}}, {'cal_date': 1, "_id": False}).sort('cal_date',
                                                                                                     ASCENDING)]


def query_trade_dates(start, end):
    busdaysPd = pd.DataFrame(DATABASE.trade_date.find({"is_open": 1}))
    busidaySe = pd.to_datetime(busdaysPd['cal_date'])
    busiday = busidaySe[(busidaySe >= start) & (busidaySe <= end)].sort_values()
    return busiday.apply(lambda t: datetime.strftime(t, '%Y%m%d'))
    # return busiday


def QA_util_to_json_from_pandas(data):
    if 'datetime' in data.columns:
        data.datetime = data.datetime.apply(str)
    if 'date' in data.columns:
        data.date = data.date.apply(str)
    return json.loads(data.to_json(orient='records'))

def date_f_str8(t):
    return datetime.strftime(t, '%Y%m%d')
