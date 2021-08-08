from datetime import datetime
import json
import pandas as pd
import sys
import tushare as ts
from BBUtils import *
import traceback

from loguru import logger
from pymongo import ASCENDING, DESCENDING

from QUANTAXIS.QAUtil import QASETTING
import time

DATABASE = QASETTING.client.quantaxis


class TsData:
    def __init__(self, truncate=False):
        if truncate:
            logger.info("重新创建collection！")
            self.tuncatate_collections()
            self._create_collections()

        token = QASETTING.get_config('TSPRO', 'token', None)
        if token is None:
            logger.error("获取tushare的token失败！")
            raise RuntimeError("获取tushare的token失败！")
        ts.set_token(token)
        self.pro = ts.pro_api(token, timeout=30000)
        print("ts_version %s" % ts.__version__)

        # if truncate:
        #     self.get_and_save_trade_calendar()

        self.stock_loop_count = 0
        self.stock_total_count = 0

        self._today_date_str = query_last_trade_date(datetime.strftime(datetime.today(), "%Y%m%d"))

        self.initStart = '20141231'
        self._start_date_str = query_next_trade_date(self.initStart)
        if not self._start_date_str:
            self._start_date_str = '20141231'

    def readCursor(self, cursor_name):
        r = DATABASE.big_brain_cursor.find_one({"cursor_name": cursor_name})
        return r["point"] if r else r

    def writeCursor(self, cursor_name, dt):
        return DATABASE.big_brain_cursor.update_one({"cursor_name": cursor_name}, {"$set": {"point": dt}},
                                                    upsert=True)

    def _create_collections(self):
        if "big_brain_cursor" not in DATABASE.list_collection_names():
            coll = DATABASE.big_brain_cursor
            coll.create_index([("cursor_name",
                                ASCENDING)], unique=True)

        '''个股信息以及相关行情数据'''
        if "tushare_stock_info" not in DATABASE.list_collection_names():
            coll = DATABASE.tushare_stock_info
            coll.create_index([("ts_code",
                                ASCENDING)], unique=True)
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
            DATABASE.tushare_money_flow.create_index([("ts_code",
                                                       ASCENDING),
                                                      ("trade_date",
                                                       ASCENDING)], unique=True)
        if "tushare_limit_list" not in DATABASE.list_collection_names():
            DATABASE.tushare_limit_list.create_index([("ts_code",
                                                       ASCENDING),
                                                      ("trade_date",
                                                       ASCENDING)], unique=True)

        if "tushare_stk_limit" not in DATABASE.list_collection_names():
            DATABASE.tushare_stk_limit.create_index([("ts_code",
                                                      ASCENDING),
                                                     ("trade_date",
                                                      ASCENDING)], unique=True)

        if "tushare_margin_detail" not in DATABASE.list_collection_names():
            DATABASE.tushare_margin_detail.create_index([("ts_code",
                                                          ASCENDING),
                                                         ("trade_date",
                                                          ASCENDING)], unique=True)

        if "tushare_stock_company" not in DATABASE.list_collection_names():
            DATABASE.tushare_stock_company.create_index([("ts_code",
                                                          ASCENDING)])

        if "tushare_top10_holders" not in DATABASE.list_collection_names():
            DATABASE.tushare_top10_holders.create_index([("ts_code", ASCENDING), ("end_date", ASCENDING)])

        if "tushare_top10_floatholders" not in DATABASE.list_collection_names():
            DATABASE.tushare_top10_floatholders.create_index([("ts_code", ASCENDING), ("end_date", ASCENDING)])

        if "tushare_stk_holdernumber" not in DATABASE.list_collection_names():
            DATABASE.tushare_stk_holdernumber.create_index([("ts_code",
                                                             ASCENDING),
                                                            ("end_date",
                                                             ASCENDING),
                                                            ("ann_date",
                                                             ASCENDING)], unique=True)

        '''财务指标数据'''
        if "tushare_fina_indicator" not in DATABASE.list_collection_names():
            DATABASE.tushare_fina_indicator.create_index([("ts_code",
                                                           ASCENDING),
                                                          ("end_date",
                                                           ASCENDING)])

        '''资产负债表'''
        if "tushare_balancesheet" not in DATABASE.list_collection_names():
            DATABASE.tushare_balancesheet.create_index([("ts_code",
                                                         ASCENDING),
                                                        ("end_date",
                                                         ASCENDING)])

        '''现金流量表'''
        if "tushare_cashflow" not in DATABASE.list_collection_names():
            DATABASE.tushare_cashflow.create_index([("ts_code",
                                                     ASCENDING),
                                                    ("end_date",
                                                     ASCENDING)])

        '''利润表'''
        if "tushare_income" not in DATABASE.list_collection_names():
            DATABASE.tushare_income.create_index([("ts_code",
                                                   ASCENDING),
                                                  ("end_date",
                                                   ASCENDING)])
        '''分红送股'''
        if "tushare_dividend" not in DATABASE.list_collection_names():
            DATABASE.tushare_dividend.create_index([("ts_code",
                                                     ASCENDING),
                                                    ("end_date",
                                                     ASCENDING)], unique=True)
        '''主营业务构成'''
        if "tushare_fina_mainbz" not in DATABASE.list_collection_names():
            DATABASE.tushare_dividend.create_index([("ts_code",
                                                     ASCENDING),
                                                    ("end_date",
                                                     ASCENDING)], unique=True)

        #
        # if "tushare_pledge_stat" not in DATABASE.list_collection_names():
        #     DATABASE.tushare_pledge_stat.create_index([("ts_code",
        #                                                   ASCENDING),
        #                                                  ("end_date",
        #                                                   ASCENDING)], unique=True)
        # if "tushare_pledge_detail" not in DATABASE.list_collection_names():
        #     DATABASE.tushare_pledge_detail.create_index([("ts_code",
        #                                                   ASCENDING),
        #                                                  ("ann_date",
        #                                                   ASCENDING)])

        '''指数行情以及成分信息等'''
        if "tushare_index_info" not in DATABASE.list_collection_names():
            DATABASE.tushare_index_info.create_index([("ts_code",
                                                       ASCENDING)], unique=True)
        if "tushare_index_day" not in DATABASE.list_collection_names():
            DATABASE.tushare_index_day.create_index([("ts_code",
                                                      ASCENDING),
                                                     ("trade_date",
                                                      ASCENDING)], unique=True)
        if "tushare_index_weight" not in DATABASE.list_collection_names():
            DATABASE.tushare_index_weight.create_index([("index_code",
                                                         ASCENDING),
                                                        ("trade_date",
                                                         ASCENDING),
                                                        ("con_code",
                                                         ASCENDING)], unique=True)
        if "tushare_index_dailybasic" not in DATABASE.list_collection_names():
            DATABASE.tushare_index_dailybasic.create_index([("ts_code",
                                                             ASCENDING),
                                                            ("trade_date",
                                                             ASCENDING)], unique=True)
        '''申万行业分类'''
        if "tushare_sw_index_classify" not in DATABASE.list_collection_names():
            DATABASE.tushare_sw_index_classify.create_index([("index_code",
                                                              ASCENDING)], unique=True)
        if "tushare_sw_index_member" not in DATABASE.list_collection_names():
            DATABASE.tushare_sw_index_member.create_index([("index_code",
                                                            ASCENDING)])
        '''同花顺指数、行情'''
        if "tushare_ths_index" not in DATABASE.list_collection_names():
            DATABASE.tushare_ths_index.create_index([("ts_code",
                                                      ASCENDING)], unique=True)
        if "tushare_ths_member" not in DATABASE.list_collection_names():
            DATABASE.tushare_ths_member.create_index([("ts_code",
                                                       ASCENDING)], unique=True)
        if "tushare_ths_daily" not in DATABASE.list_collection_names():
            DATABASE.tushare_ths_daily.create_index([("ts_code",
                                                      ASCENDING),
                                                     ("trade_date",
                                                      ASCENDING)], unique=True)

        '''融资融券交易汇总'''
        if "tushare_margin" not in DATABASE.list_collection_names():
            DATABASE.tushare_margin.create_index([("trade_date", ASCENDING), ("exchange_id", ASCENDING)], unique=True)

        '''龙虎榜每日明细'''
        if "tushare_top_list" not in DATABASE.list_collection_names():
            DATABASE.top_list.create_index([("ts_code",
                                             ASCENDING),
                                            ("trade_date",
                                             ASCENDING)], unique=True)
        '''龙虎榜机构明细'''
        if "tushare_top_inst" not in DATABASE.list_collection_names():
            DATABASE.top_inst.create_index([("trade_date", ASCENDING)])

        #
        # '''基金信息'''
        # if "tushare_fund_basic" not in DATABASE.list_collection_names():
        #     DATABASE.tushare_fund_basic.create_index([("ts_code", ASCENDING)], unique=True)
        #
        # '''基金日行情'''
        # if "tushare_fund_daily" not in DATABASE.list_collection_names():
        #     DATABASE.tushare_fund_daily.create_index([("ts_code", ASCENDING), ("trade_date", ASCENDING)], unique=True)
        #
        # '''基金复权因子'''
        # if "tushare_fund_adj" not in DATABASE.list_collection_names():
        #     DATABASE.tushare_fund_adj.create_index([("ts_code", ASCENDING), ("trade_date", ASCENDING)], unique=True)
        #

        # '''交易日历'''
        # if "trade_date" not in DATABASE.list_collection_names():
        #     coll = DATABASE.trade_date
        #     coll.create_index([("cal_date",
        #                         ASCENDING)], unique=True)

    def tuncatate_collections(self):
        for i in DATABASE.list_collection_names():
            if i.startswith("tushare"):
                DATABASE.drop_collection(i)
        DATABASE.drop_collection('big_brain_cursor')

    def get_and_save_trade_calendar(self):
        '''交易日历'''
        df = self.pro.trade_cal(exchange='SSE', start_date='19900101', end_date='20211231', is_open='1')
        DATABASE.trade_date.insert_many(QA_util_to_json_from_pandas(df))

    def get_and_save_stock_list(self):
        '''股票列表'''
        df = self.pro.stock_basic(fields='ts_code,symbol,name,area,industry,list_date,market,list_status,is_hs')
        df_tuishi = self.pro.stock_basic(list_status='D',
                                         fields='ts_code,symbol,name,area,industry,list_date,market,list_status,is_hs')
        df_zanting = self.pro.stock_basic(list_status='P',
                                          fields='ts_code,symbol,name,area,industry,list_date,market,list_status,is_hs')

        df = df.append(df_tuishi)
        df = df.append(df_zanting)
        try:
            if df.shape[0]:

                DATABASE.tushare_stock_info.insert_many(QA_util_to_json_from_pandas(df))
            else:
                logger.error("get tushare stock_info error")
        except:
            pass

    def get_and_save_index_list(self):
        '''指数列表'''
        df = self.pro.index_basic()
        if df.shape[0]:
            DATABASE.tushare_index_info.insert_many(QA_util_to_json_from_pandas(df))
        else:
            logger.error("get tushare index_basic error")

    def query_stock_list(self, code=None):
        qdict = {}
        if (code is not None):
            qdict['ts_code'] = code
        return pd.DataFrame(DATABASE.tushare_stock_info.find(qdict))

    def query_index_list(self, code=None):
        qdict = {}
        if (code is not None):
            qdict['ts_code'] = code
        return pd.DataFrame(DATABASE.tushare_index_info.find(qdict))

    def query_ths_index_list(self, code=None):
        qdict = {}
        if (code is not None):
            qdict['ts_code'] = code
        return pd.DataFrame(DATABASE.tushare_ths_index.find(qdict))

    def _query_last_date_stock_day_qfq(self, ts_code):
        '''取股票代码的离当前时间的最近日线记录'''
        data = list(DATABASE.tushare_stock_day.find({'ts_code': ts_code}).sort('trade_date', DESCENDING).limit(1))
        if len(data) > 0:
            return data[0]
        else:
            return None

    def get_and_save_sw_index_classify(self):
        df = self.pro.index_classify()
        if not df.empty:
            DATABASE.tushare_sw_index_classify.insert_many(QA_util_to_json_from_pandas(df))
            logger.info("get tushare_sw_index ")
            df.apply(self.get_and_save_sw_index_member, axis=1)

        else:
            logger.error("get tushare sw_index_classify error")

    def get_and_save_sw_index_member(self, item):
        time.sleep(0.3)
        index_code = item['index_code']
        dt = DATABASE.tushare_sw_index_member.find_one({"index_code": index_code})
        if not dt or (dt.__contains__("cursor_dt") and dt["cursor_dt"] > self._today_date_str):
            df = self.pro.index_member(index_code=index_code)
            if not df.empty:
                DATABASE.tushare_sw_index_member.insert_many(QA_util_to_json_from_pandas(df))
                DATABASE.tushare_sw_index_classify.update_one({"ts_code": index_code}, {'$set': {'cursor_dt': self._today_date_str}}, True)
                logger.info("get tushare_sw_index member = {} ".format(index_code))
            else:
                logger.error("get tushare sw_index_member error")

    def get_and_save_ths_index(self):
        df = self.pro.ths_index()
        if df.shape[0]:
            DATABASE.tushare_ths_index.insert_many(QA_util_to_json_from_pandas(df))
        else:
            logger.error("get tushare ths_index error")

    # def get_and_save_ths_member(self):
    #     df = self.pro.ths_member()
    #     if df.shape[0]:
    #         DATABASE.tushare_ths_member.insert_many(QA_util_to_json_from_pandas(df))
    #     else:
    #         logger.error("get tushare ths_member error")

    def save_by_code_date(self, stock, func, colleciton_name):
        try:
            stock_code = stock['ts_code']
            cursorname = colleciton_name + "-" + stock_code
            dt = self.readCursor(cursorname)
            start_date = query_next_trade_date(dt) if dt else stock["list_date"]
            start_date = self.initStart if start_date <= self.initStart else start_date
            if start_date <= self._today_date_str:
                res = func(ts_code=stock_code, start_date=start_date, end_date=self._today_date_str)
                logger.info("get {} = {}, from {} to {}".format(colleciton_name, stock_code, start_date, self._today_date_str))
                if not res.empty:
                    DATABASE[colleciton_name].insert_many(QA_util_to_json_from_pandas(res))
                self.writeCursor(cursorname, self._today_date_str)
        except Exception as e:
            logger.warning("ERROR get {} = {}, from {} to {}".format(colleciton_name, stock_code, start_date, self._today_date_str))
            traceback.print_exc()

    def save_by_code(self, stock, func, colleciton_name):
        try:
            stock_code = stock['ts_code']
            cursorname = colleciton_name + "-" + stock_code
            dt = self.readCursor(cursorname)
            start_date = query_next_trade_date(dt) if dt else self.initStart
            if start_date <= self._today_date_str:
                res = func(ts_code=stock_code)
                logger.info("get {} = {}".format(colleciton_name, stock_code))
                if not res.empty:
                    DATABASE[colleciton_name].insert_many(QA_util_to_json_from_pandas(res))
                self.writeCursor(cursorname, self._today_date_str)
        except Exception as e:
            logger.warning("ERROR get {} = {}".format(colleciton_name, stock_code))
            traceback.print_exc()

    def _query_and_save_stock_day(self, item):
        '''
            保存个股相关的每日信息
        Args:
            item:

        Returns:

        '''
        try:
            self.stock_loop_count += 1
            stock_code = item['ts_code']
            last_date = self._start_date_str
            if last_date < item['list_date']:
                last_date = item['list_date']
            ipo_date = item['list_date']  # Initial public offering

            default_start_date = self._start_date_str if ipo_date < self._start_date_str else ipo_date
            # dt = self.readCursor(self.__class__.__name__+sys._getframe().f_code.co_name+"-"+stock_code)
            # if dt and dt > last_date:
            #     last_date = dt

            logger.info("stock {}/{}", self.stock_loop_count, self.stock_total_count)
            dt = self.readCursor("tushare_stock_day" + "-" + stock_code)
            last_date = query_next_trade_date(dt) if dt else default_start_date
            if last_date <= self._today_date_str:
                '''个股日线数据'''
                stock_day_data = ts.pro_bar(ts_code=stock_code, adj='qfq', start_date=last_date, end_date=self._today_date_str, adjfactor=True, freq="D")
                logger.info("get stock_day_code = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                DATABASE.tushare_stock_day.insert_many(QA_util_to_json_from_pandas(stock_day_data))
                self.writeCursor("tushare_stock_day" + "-" + stock_code, self._today_date_str)

            self.save_by_code_date(item, self.pro.daily_basic, "tushare_baisc_daily")
            self.save_by_code_date(item, self.pro.moneyflow, "tushare_moneyflow")
            self.save_by_code_date(item, self.pro.limit_list, "tushare_limit_list")
            self.save_by_code_date(item, self.pro.stk_limit, "tushare_stk_limit")
            self.save_by_code_date(item, self.pro.margin_detail, "tushare_margin_detail")  # '''融资融券交易明细'''
            self.save_by_code_date(item, self.pro.top10_holders, "tushare_top10_holders")  # '''前十大股东'''
            self.save_by_code_date(item, self.pro.top10_floatholders, "tushare_top10_floatholders")  # '''前十大流通股东'''
            self.save_by_code_date(item, self.pro.stk_managers, "tushare_stk_managers")  # 上市公司管理层
            self.save_by_code_date(item, self.pro.fina_indicator, "tushare_fina_indicator")  # '''财务指标数据'''
            self.save_by_code_date(item, self.pro.cashflow, "tushare_cashflow")
            self.save_by_code_date(item, self.pro.balancesheet, "tushare_moneyflow")  # '''资产负债'''
            self.save_by_code_date(item, self.pro.income, "tushare_income")  # 利润表
            self.save_by_code_date(item, self.pro.fina_mainbz, "tushare_fina_mainbz")  # 主营业务构成
            self.save_by_code_date(item, self.pro.stk_holdernumber, "tushare_stk_holdernumber")  # 上市公司股东户数

            self.save_by_code(item, self.pro.stk_rewards, "tushare_stk_rewards")  # 上市公司管理层薪酬
            # self.save_by_code(item, self.pro.stock_company, "tushare_stock_company")  # 市公司基本信息


        except Exception as e:
            logger.warning("stock get history error = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
            traceback.print_exc()

    def _query_and_save_index_day(self, item):
        '''
            保存指数相关的信息
        Args:
            item:

        Returns:

        '''
        self.stock_loop_count += 1
        self.save_by_code_date(item, self.pro.index_daily, "tushare_index_day")
        self.save_by_code_date(item, self.pro.index_weight, "tushare_index_weight")
        self.save_by_code_date(item, self.pro.index_dailybasic, "tushare_index_dailybasic")


    def _query_and_save_ths_index_day(self, item):
        '''
            保存同花顺指数相关的信息
        Args:
            item:

        Returns:

        '''
        try:
            self.stock_loop_count += 1
            stock_code = item['ts_code']
            last_date = self._start_date_str

            ipo_date = item['list_date']  # Initial public offering
            default_start_date = self._start_date_str if ipo_date < self._start_date_str else ipo_date

            dt = self.readCursor("ths_daily" + "-" + stock_code)
            last_date = dt if dt else default_start_date

            if last_date <= self._today_date_str:
                logger.info("index：{}/{}", self.stock_loop_count, self.stock_total_count)

                '''褪黑素指标日线数据'''
                ths_daily = self.pro.ths_daily(ts_code=stock_code, start_date=last_date,
                                               end_date=self._today_date_str)
                logger.info(
                    "get ths_daily code = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                DATABASE.tushare_ths_daily.insert_many(QA_util_to_json_from_pandas(ths_daily))
                self.writeCursor("tushare_ths_daily" + "-" + stock_code, self._today_date_str)
            '''指标成分权重,需要5000积分暂时没有权限'''
            # ths_member = self.pro.ths_member(ts_code=stock_code)
            # logger.info("get ths_member code = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
            # DATABASE.tushare_ths_member.insert_many(QA_util_to_json_from_pandas(ths_member))

        except Exception as e:
            logger.warning("ths_index get history error = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
            traceback.print_exc()

    # '''
    #     1、股票代码
    #     2、开始时间
    #     3、结束时间
    #     根据3个条件获取历史数据并保存
    # '''
    # def save_stock_day_history(self):
    #     '''根据股票列表全量保存历史的数据'''
    #     stock_list_df = self.query_stock_list()
    #     if not stock_list_df.shape[0]:
    #         self.save_stock_list(self.get_stock_list())
    #         stock_list_df = self.query_stock_list()
    #     self.stock_total_count = stock_list_df.shape[0]
    #     stock_list_df.apply(self._query_and_save_stock_day, axis=1)

    def save_init_1(self):
        logger.info("start 初始化历史数据")
        '''股票列表'''
        self.get_and_save_stock_list()
        stock_list_df = self.query_stock_list()
        '''个股相关的信息'''
        self.stock_total_count = stock_list_df.shape[0]
        stock_list_df.apply(self._query_and_save_stock_day, axis=1)

        '''指数列表'''
        self.get_and_save_index_list()
        index_list_df = self.query_index_list()
        self.stock_total_count = index_list_df.shape[0]
        self.stock_loop_count = 0
        index_list_df.apply(self._query_and_save_index_day, axis=1)

        '''同花顺概念和行业指数'''
        self.get_and_save_ths_index()
        ths_index_list_df = self.query_ths_index_list()
        self.stock_total_count = ths_index_list_df.shape[0]
        self.stock_loop_count = 0
        ths_index_list_df.apply(self._query_and_save_ths_index_day, axis=1)

        '''申万行业分类'''
        self.get_and_save_sw_index_classify()

        '''整体数据，按照交易日获取的'''
        self._save_init_dailys()

    def _save_init_dailys(self):
        '''日期范围获取'''
        # margin = self.pro.margin(start_date=self._start_date_str, end_date=self._today_date_str)
        # logger.info("get margin = from {} to {}".format(self._start_date_str, self._today_date_str))
        # DATABASE.tushare_margin.insert_many(QA_util_to_json_from_pandas(margin))

        '''按日期循环获取'''
        tradedate = self.readCursor("tushare_get_by_date")
        tradedate = query_next_trade_date(tradedate if tradedate else self._start_date_str)
        while tradedate <= self._today_date_str:
            time.sleep(0.1)
            '''龙虎榜每日明细'''
            top_list = self.pro.query_trade_dates(trade_date=tradedate)
            logger.info("get top_list dt = {}".format(tradedate))
            if not top_list.empty:
                DATABASE.tushare_top_list.insert_many(QA_util_to_json_from_pandas(top_list))

            # if dt > '20180101':
            '''龙虎榜机构明细'''
            top_inst = self.pro.top_inst(trade_date=tradedate)
            logger.info("get top_inst dt = {}".format(tradedate))
            if not top_inst.empty:
                DATABASE.tushare_top_inst.insert_many(QA_util_to_json_from_pandas(top_inst))
            self.writeCursor("tushare_get_by_date", tradedate)
            tradedate = query_next_trade_date(tradedate)


if __name__ == '__main__':
    # print(TsData())

    # df = ts1.pro.index_basic()
    # df.to_excel('/Users/chenyuying/securecrt/index_basic.xlsx', index=True)
    # ts1.save_trade_calendar(ts1.get_trade_calendar())

    # ts.save_stock_list_calendar(ts.get_stock_list_calendar())

    # df = ts1.query_stock_list()

    # df = ts1.get_stock_day_qfq('000001.SZ', '19900101', '20210701')
    # ts1.save_stock_day_qfq(df)

    # print(ts1.query_next_all_trade_date('20210715'))

    # ts1.save_all_stock_day_history()
    # print(type(ts1.pro.daily_basic(trade_date='20210723')))
    # ts1.save_by_trade_date_history()
    # ts1.tuncatate_collections()
    # ts1.save_init_1()

    # df = ts1.pro.fina_indicator(ts_code = '000001.SZ', start_date=ts1._start_date_str, end_date=ts1._today_date_str)
    # print(df)
    #
    ts1 = TsData()
    ts1.save_init_1()
