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
        token = QASETTING.get_config('TSPRO', 'token', None)
        if token is None:
            logger.error("获取tushare的token失败！")
            raise RuntimeError("获取tushare的token失败！")
        ts.set_token(token)
        self.pro = ts.pro_api(token)
        print("ts_version %s" % ts.__version__)

    def get_trade_calendar(self):
        return self.pro.trade_cal()

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
        return ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date, end_date=end_date, adjfactor=True)

    def save_stock_day_qfq(self, data):
        DATABASE.drop_collection('tushare_stock_day')
        coll = DATABASE.tushare_stock_day
        coll.create_index([("ts_code",
                            ASCENDING),
                           ("trade_date",
                            ASCENDING)], unique=True)
        coll.insert_many(QA_util_to_json_from_pandas(data))

    def _query_last_stock_day_qfq(self, ts_code):
        return DATABASE.tushare_stock_day.find({'ts_code', ts_code}).sort('trade_date', DESCENDING).limit(1)

    def _query_and_save_stock_day(self, item):
        self._query_last_stock_day_qfq(item['ts_code'])

    def save_all_stock_day_history(self):
        stock_list_df = self.query_stock_list()
        stock_list_df.apply(self._query_and_save_stock_day, axis=1)


if __name__ == '__main__':
    # print(TsData())
    ts1 = TsData()

    # ts.save_stock_list_calendar(ts.get_stock_list_calendar())

    # df = ts1.query_stock_list()

    # df = ts1.get_stock_day_qfq('000001.SZ', '19900101', '20210701')
    # ts1.save_stock_day_qfq(df)
    ts1.save_all_stock_day_history()
    # print(df)
