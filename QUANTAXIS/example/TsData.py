from datetime import datetime
import json
import pandas as pd
import tushare as ts
from loguru import logger
from pymongo import ASCENDING, DESCENDING

from QUANTAXIS import QA_util_to_json_from_pandas
from QUANTAXIS.QAUtil import QASETTING

DATABASE = QASETTING.client.quantaxis


class TsData:
    def __init__(self):

        # DATABASE.drop_collection('tushare_stock_day')
        if "tushare_stock_day" not in DATABASE.list_collection_names():
            coll = DATABASE.tushare_stock_day
            coll.create_index([("ts_code",
                                ASCENDING),
                               ("trade_date",
                                ASCENDING)], unique=True)
        if "tushare_daily_basic" not in DATABASE.list_collection_names():
            coll = DATABASE.tushare_daily_basic
            coll.create_index([("ts_code",
                                ASCENDING),
                               ("trade_date",
                                ASCENDING)], unique=True)
        if "tushare_money_flow" not in DATABASE.list_collection_names():
            DATABASE.tushare_money_flow.coll.create_index([("ts_code",
                                ASCENDING),
                               ("trade_date",
                                ASCENDING)], unique=True)
        if "tushare_limit_list" not in DATABASE.list_collection_names():
            DATABASE.tushare_limit_list.coll.create_index([("ts_code",
                                ASCENDING),
                               ("trade_date",
                                ASCENDING)], unique=True)

        if "trade_date" not in DATABASE.list_collection_names():
            coll = DATABASE.trade_date
            coll.create_index([("cal_date",
                                ASCENDING)], unique=True)

        token = QASETTING.get_config('TSPRO', 'token', None)
        if token is None:
            logger.error("获取tushare的token失败！")
            raise RuntimeError("获取tushare的token失败！")
        ts.set_token(token)
        self.pro = ts.pro_api(token)
        print("ts_version %s" % ts.__version__)

        self.stock_loop_count = 0
        self.stock_total_count = 0

        self._today_date_str = datetime.strftime(datetime.today(), "%Y%m%d")
        last_trade_date = self.query_last_trade_date(self._today_date_str)

        if self._today_date_str > last_trade_date:
            self._today_date_str = last_trade_date

    def get_trade_calendar(self):
        return self.pro.trade_cal(exchange='SSE', start_date='19900101', end_date='20211231', is_open='1')

    def save_trade_calendar(self, data):
        DATABASE.trade_date.insert_many(QA_util_to_json_from_pandas(data))

    def get_stock_list(self):
        '''从tushare获取股票列表'''
        return self.pro.stock_basic(fields='ts_code,symbol,name,area,industry,list_date,market,list_status,is_hs')

    def save_stock_list(self, data):
        '''保存获取股票列表'''
        DATABASE.drop_collection('tushare_stock_info')
        coll = DATABASE.tushare_stock_info
        coll.create_index('ts_code')
        coll.insert_many(QA_util_to_json_from_pandas(data))

    def query_stock_list(self, code=None):
        qdict = {}
        if (code is not None):
            qdict['ts_code'] = code
        return pd.DataFrame(DATABASE.tushare_stock_info.find(qdict))

    def get_stock_day_qfq(self, ts_code, start_date, end_date):
        return ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date, end_date=end_date, adjfactor=True,
                          freq="D")

    def save_stock_day_qfq(self, data):
        DATABASE.tushare_stock_day.insert_many(QA_util_to_json_from_pandas(data))

    def _query_last_date_stock_day_qfq(self, ts_code):
        '''取股票代码的离当前时间的最近日线记录'''
        data = list(DATABASE.tushare_stock_day.find({'ts_code': ts_code}).sort('trade_date', DESCENDING).limit(1))
        if len(data) > 0:
            return data[0]
        else:
            return None

    def _query_and_save_stock_day(self, item):

        self.stock_loop_count += 1
        stock_code = item['ts_code']
        last_date = self._query_last_date_stock_day_qfq(stock_code)
        if not last_date:
            last_date = item['list_date']
        else:
            last_date = self.query_next_trade_date(last_date['trade_date'])

        if last_date <= self._today_date_str:
            logger.info("{}/{}", self.stock_loop_count, self.stock_total_count)

            stock_day_data = self.get_stock_day_qfq(stock_code, last_date, self._today_date_str)
            logger.info("get stock_day_code = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
            DATABASE.tushare_stock_day.insert_many(QA_util_to_json_from_pandas(stock_day_data))

            stock_baisc_daily = self.pro.daily_basic(ts_code=stock_code, start_date=last_date,
                                                     end_date=self._today_date_str)
            logger.info("get daily_basic_code = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
            DATABASE.tushare_baisc_daily.insert_many(QA_util_to_json_from_pandas(stock_baisc_daily))

    def query_and_save_daily_basic_by_day(self, item):
        stock_code = item['ts_code']
        last_date = list(
            DATABASE.tushare_daily_basic.find({'ts_code': stock_code}).sort('trade_date', DESCENDING).limit(1))
        if not last_date:
            last_date = item['list_date']
        else:
            last_date = self.query_next_trade_date(last_date['trade_date'])

        if last_date <= self._today_date_str:
            stock_day_data = self.get_stock_day_qfq(stock_code, last_date, self._today_date_str)
            logger.info("get stock_code = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
            DATABASE.tushare_stock_day.insert_many(QA_util_to_json_from_pandas(stock_day_data))

    def save_all_stock_day_history(self):
        '''根据股票和交易日期全量保存数据'''
        stock_list_df = self.query_stock_list()
        if not stock_list_df.shape[0]:
            self.save_stock_list(self.get_stock_list())
            stock_list_df = self.query_stock_list()
        self.stock_total_count = stock_list_df.shape[0]
        stock_list_df.apply(self._query_and_save_stock_day, axis=1)

    def query_last_trade_date(self, dt):
        '''查询上一个交易日，如果传入的当日是交易日，则算传入的日期'''
        return (list(DATABASE.trade_date.find({'cal_date': {'$lte': dt}}).sort('cal_date', DESCENDING).limit(1)))[0][
            'cal_date']

    def query_next_trade_date(self, dt):
        '''查询传入参数日期的下一个交易日，如果传入的当日是交易日，不算'''
        return (list(DATABASE.trade_date.find({'cal_date': {'$gt': dt}}).sort('cal_date', DESCENDING).limit(1)))[0][
            'cal_date']

    def query_next_all_trade_date(self, dt):
        '''查询传入参数日期的下一个交易日，如果传入的当日是交易日，不算'''
        return [item['cal_date'] for item in
                DATABASE.trade_date.find({'cal_date': {'$gt': dt}}, {'cal_date': 1, "_id": False}).sort('cal_date',
                                                                                                        ASCENDING)]
    def query_before_all_trade_date(self, dt):
        '''查询传入参数日期的上所有交易日，如果传入的当日是交易日也算上'''
        return [item['cal_date'] for item in
                DATABASE.trade_date.find({'cal_date': {'$lte': dt}}, {'cal_date': 1, "_id": False}).sort('cal_date',
                                                                                                        ASCENDING)]


    def save_data_by_trade_date(self,dt):
        '''根据交易日期保存数据'''
        DATABASE.tushare_daily_basic.insert_many(QA_util_to_json_from_pandas(self.pro.daily_basic(trade_date=dt)))
        DATABASE.tushare_money_flow.insert_many(QA_util_to_json_from_pandas(self.pro.moneyflow(trade_date=dt)))
        DATABASE.tushare_limit_list.insert_many(QA_util_to_json_from_pandas(self.pro.limit_list(trade_date=dt)))


    def save_data_by_trade_date_history(self):
        for item in self.query_before_all_trade_date(datetime.strftime(datetime.today(), "%Y%m%d")):
            self.save_data_by_trade_date(item)



if __name__ == '__main__':
    # print(TsData())
    ts1 = TsData()
    # ts1.save_trade_calendar(ts1.get_trade_calendar())

    # ts.save_stock_list_calendar(ts.get_stock_list_calendar())

    # df = ts1.query_stock_list()

    # df = ts1.get_stock_day_qfq('000001.SZ', '19900101', '20210701')
    # ts1.save_stock_day_qfq(df)

    # print(ts1.query_next_all_trade_date('20210715'))

    # ts1.save_all_stock_day_history()
    # print(type(ts1.pro.daily_basic(trade_date='20210723')))
    ts1.save_data_by_trade_date_history()