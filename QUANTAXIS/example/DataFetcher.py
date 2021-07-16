from QUANTAXIS import QA_Setting
import pandas as pd
from QUANTAXIS.QAFetch.QAQuery_Advance import *

QASETTING = QA_Setting()
DATABASE = QASETTING.client.quantaxis


# def getAllTradeCal():
#     return pd.DataFrame(DATABASE.trade_date.find({"is_open": 1}))


class MongoDataLoader:
    def __init__(self):
        pass

    def load_stock_day(self,
                       code,
                       start='all',
                       end=None
                       ):
        QA_fetch_stock_day_adv(code, start, end)

    def load_stock_list(self):
        return QA_fetch_stock_list_adv()

    def load_trade_cal(self):
        return pd.DataFrame(DATABASE.trade_date.find({"is_open": 1}))

    def load_stock_day_full(self,date):
        return QA_fetch_stock_day_full_adv(date)


if __name__ == '__main__':
    print(MongoDataLoader().load_trade_cal())
