from datetime import datetime
import json
import pandas as pd
import sys
import tushare as ts
from BBUtils import *
import traceback

from loguru import logger
from pymongo import ASCENDING, DESCENDING

from QUANTAXIS import QA_util_to_json_from_pandas
from QUANTAXIS.QAUtil import QASETTING
import time
DATABASE = QASETTING.client.quantaxis


class TsData:
    def __init__(self, truncate=False):
        if truncate:
            logger.warn("重新创建collection！")
            self.tuncatate_collections()
            self._create_collections()

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
        last_trade_date = query_last_trade_date(self._today_date_str)
        if self._today_date_str > last_trade_date:
            self._today_date_str = last_trade_date

        initStart = '20141231'
        self._start_date_str = query_next_trade_date(initStart)
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

        '''交易日历'''
        if "trade_date" not in DATABASE.list_collection_names():
            coll = DATABASE.trade_date
            coll.create_index([("cal_date",
                                ASCENDING)], unique=True)

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

            # dt = self.readCursor(self.__class__.__name__+sys._getframe().f_code.co_name+"-"+stock_code)
            # if dt and dt > last_date:
            #     last_date = dt
            dt = DATABASE.tushare_stock_info.find_one({"ts_code": stock_code})
            if dt and dt.__contains__("stock_cursor_dt") and dt["stock_cursor_dt"] > last_date:
                last_date = query_next_trade_date(dt)

            if last_date <= self._today_date_str:
                logger.info("stock {}/{}", self.stock_loop_count, self.stock_total_count)

                '''个股日线数据'''
                stock_day_data = ts.pro_bar(ts_code=stock_code, adj='qfq', start_date=last_date,
                                            end_date=self._today_date_str, adjfactor=True,
                                            freq="D")
                logger.info(
                    "get stock_day_code = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                DATABASE.tushare_stock_day.insert_many(QA_util_to_json_from_pandas(stock_day_data))

                '''个股基础指标'''
                stock_baisc_daily = self.pro.daily_basic(ts_code=stock_code, start_date=last_date,
                                                         end_date=self._today_date_str)
                logger.info(
                    "get daily_basic_code = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                DATABASE.tushare_baisc_daily.insert_many(QA_util_to_json_from_pandas(stock_baisc_daily))

                '''个股资金流向'''
                money_flow = self.pro.moneyflow(ts_code=stock_code, start_date=last_date, end_date=self._today_date_str)
                logger.info("get money_flow = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                DATABASE.tushare_money_flow.insert_many(QA_util_to_json_from_pandas(money_flow))

                '''每日涨跌停统计'''
                limit_list = self.pro.limit_list(ts_code=stock_code, start_date=last_date,
                                                 end_date=self._today_date_str)
                logger.info("get limit_list = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                DATABASE.tushare_limit_list.insert_many(QA_util_to_json_from_pandas(limit_list))

                '''每日涨跌停价格'''
                stk_limit = self.pro.stk_limit(ts_code=stock_code, start_date=last_date, end_date=self._today_date_str)
                logger.info("get stk_limit = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                DATABASE.tushare_stk_limit.insert_many(QA_util_to_json_from_pandas(stk_limit))

                '''融资融券交易明细'''
                margin_detail = self.pro.margin_detail(ts_code=stock_code, start_date=last_date,
                                                       end_date=self._today_date_str)
                logger.info("get margin_detail = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                if not margin_detail.empty:
                    DATABASE.tushare_margin_detail.insert_many(QA_util_to_json_from_pandas(margin_detail))

                '''前十大股东'''
                top10_holders = self.pro.top10_holders(ts_code=stock_code, start_date=last_date,
                                                       end_date=self._today_date_str)
                logger.info("get top10_holders = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                DATABASE.tushare_top10_holders.insert_many(QA_util_to_json_from_pandas(top10_holders))

                '''前十大流通股东'''
                top10_floatholders = self.pro.top10_floatholders(ts_code=stock_code, start_date=last_date,
                                                                 end_date=self._today_date_str)
                logger.info(
                    "get top10_floatholders = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                DATABASE.tushare_top10_floatholders.insert_many(QA_util_to_json_from_pandas(top10_floatholders))

                '''上市公司管理层'''
                stk_managers = self.pro.stk_managers(ts_code=stock_code, start_date=last_date,
                                                     end_date=self._today_date_str)
                logger.info("get stk_managers = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                DATABASE.tushare_stk_managers.insert_many(QA_util_to_json_from_pandas(stk_managers))

                '''财务指标数据'''
                fina_indicator = self.pro.fina_indicator(ts_code=stock_code, start_date=last_date,
                                                         end_date=self._today_date_str)
                logger.info(
                    "get fina_indicator = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                DATABASE.tushare_fina_indicator.insert_many(QA_util_to_json_from_pandas(fina_indicator))

                '''现金流量表'''
                cashflow = self.pro.cashflow(ts_code=stock_code, start_date=last_date,
                                             end_date=self._today_date_str)
                logger.info("get cashflow = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                DATABASE.tushare_cashflow.insert_many(QA_util_to_json_from_pandas(cashflow))

                '''资产负债'''
                balancesheet = self.pro.balancesheet(ts_code=stock_code, start_date=last_date,
                                                     end_date=self._today_date_str)
                logger.info("get balancesheet = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                DATABASE.tushare_balancesheet.insert_many(QA_util_to_json_from_pandas(balancesheet))

                '''利润表'''
                income = self.pro.income(ts_code=stock_code, start_date=last_date,
                                         end_date=self._today_date_str)
                logger.info("get income = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                DATABASE.tushare_income.insert_many(QA_util_to_json_from_pandas(income))

                '''主营业务构成'''
                fina_mainbz = self.pro.fina_mainbz(ts_code=stock_code, start_date=last_date,
                                                   end_date=self._today_date_str)
                logger.info("get fina_mainbz = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                DATABASE.tushare_fina_mainbz.insert_many(QA_util_to_json_from_pandas(fina_mainbz))

                '''上市公司股东户数'''
                stk_holdernumber = self.pro.stk_holdernumber(ts_code=stock_code, start_date=self._start_date_str,
                                                             end_date=self._today_date_str)
                logger.info("get stk_holdernumber = from {} to {}".format(self._start_date_str, self._today_date_str))
                DATABASE.tushare_stk_holdernumber.insert_many(QA_util_to_json_from_pandas(stk_holdernumber))

            '''不需要cursor信息的数据'''
            '''上市公司管理层薪酬'''
            stk_rewards = self.pro.stk_rewards(ts_code=stock_code)
            logger.info("get stk_rewards = {}".format(stock_code))
            DATABASE.tushare_stk_rewards.insert_many(QA_util_to_json_from_pandas(stk_rewards))

            '''上市公司基本信息'''
            stock_company = ts.pro_bar(ts_code=stock_code)
            logger.info("get stock_company = {}".format(stock_code))
            DATABASE.tushare_stock_company.insert_many(QA_util_to_json_from_pandas(stock_company))

            DATABASE.tushare_stock_info.update_one({"ts_code": stock_code}, {'$set': {'stock_cursor_dt': self._today_date_str}}, True)

        except Exception as e:
            logger.info(
                "stock get history error = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
            traceback.print_exc()



    def _query_and_save_index_day(self, item):
        '''
            保存指数相关的信息
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

            if last_date <= self._today_date_str:
                logger.info("index：{}/{}", self.stock_loop_count, self.stock_total_count)

                '''指标日线数据'''
                tushare_index_day = self.pro.index_daily(ts_code=stock_code, start_date=last_date,
                                                         end_date=self._today_date_str)

                logger.info(
                    "get index_day code = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                DATABASE.tushare_index_day.insert_many(QA_util_to_json_from_pandas(tushare_index_day))

                '''指标成分权重'''
                index_weight = self.pro.index_weight(index_code=stock_code, start_date=last_date,
                                                     end_date=self._today_date_str)
                logger.info(
                    "get index_weight code = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                DATABASE.tushare_index_weight.insert_many(QA_util_to_json_from_pandas(index_weight))

                '''指数每日指标，目前只提供上证综指，深证成指，上证50，中证500，中小板指，创业板指的每日指标数据'''
                index_dailybasic = self.pro.index_dailybasic(ts_code=stock_code, start_date=last_date,
                                                             end_date=self._today_date_str)
                logger.info(
                    "get index_dailybasic = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                if not index_dailybasic.empty:
                    DATABASE.tushare_index_dailybasic.insert_many(QA_util_to_json_from_pandas(index_dailybasic))
        except Exception as e:
            logger.info(
                "index get history error = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))

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
            if last_date < item['list_date']:
                last_date = item['list_date']

            if last_date <= self._today_date_str:
                logger.info("index：{}/{}", self.stock_loop_count, self.stock_total_count)

                '''指标日线数据'''
                ths_daily = self.pro.ths_daily(ts_code=stock_code, start_date=last_date,
                                               end_date=self._today_date_str)
                logger.info(
                    "get ths_daily code = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
                DATABASE.tushare_index_day.insert_many(QA_util_to_json_from_pandas(ths_daily))

            '''指标成分权重,需要5000积分暂时没有权限'''
            # ths_member = self.pro.ths_member(ts_code=stock_code)
            # logger.info("get ths_member code = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))
            # DATABASE.tushare_ths_member.insert_many(QA_util_to_json_from_pandas(ths_member))
        except Exception as e:
            logger.info(
                "ths_index get history error = {}, from {} to {}".format(stock_code, last_date, self._today_date_str))

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
        # '''股票列表'''
        # self.get_and_save_stock_list()
        # stock_list_df = self.query_stock_list()
        # '''个股相关的信息'''
        # self.stock_total_count = stock_list_df.shape[0]
        # stock_list_df.apply(self._query_and_save_stock_day, axis=1)
        #
        # '''指数列表'''
        # self.get_and_save_index_list()
        # index_list_df = self.query_index_list()
        # self.stock_total_count = index_list_df.shape[0]
        # index_list_df.apply(self._query_and_save_index_day, axis=1)
        #
        # '''同花顺概念和行业指数'''
        # self.get_and_save_ths_index()
        # ths_index_list_df = self.query_ths_index_list()
        # self.stock_total_count = ths_index_list_df.shape[0]
        # ths_index_list_df.apply(self._query_and_save_ths_index_day, axis=1)

        '''申万行业分类'''
        # self.get_and_save_sw_index_classify()

        '''整体数据，按照交易日获取的'''
        self._save_init_dailys()

    def _save_init_dailys(self):
        '''日期范围获取'''
        # margin = self.pro.margin(start_date=self._start_date_str, end_date=self._today_date_str)
        # logger.info("get margin = from {} to {}".format(self._start_date_str, self._today_date_str))
        # DATABASE.tushare_margin.insert_many(QA_util_to_json_from_pandas(margin))

        '''按日期循环获取'''
        for dt in query_trade_dates(self._start_date_str, self._today_date_str):
            time.sleep(0.1)
            '''龙虎榜每日明细'''
            top_list = self.pro.top_list(trade_date=dt)
            logger.info("get top_list dt = {}".format(dt))
            if not top_list.empty:
                DATABASE.tushare_top_list.insert_many(QA_util_to_json_from_pandas(top_list))
            # if dt > '20180101':
            '''龙虎榜机构明细'''
            top_inst = self.pro.top_inst(trade_date=dt)
            logger.info("get top_inst dt = {}".format(dt))
            if not top_inst.empty:
                DATABASE.tushare_top_inst.insert_many(QA_util_to_json_from_pandas(top_inst))


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
