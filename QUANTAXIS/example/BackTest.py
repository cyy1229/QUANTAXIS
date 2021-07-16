from QUANTAXIS import QA_User, QA_Setting
import pandas as pd
from DataFetcher import MongoDataLoader
from datetime import datetime


class Strategy:

    def __init__(self, strategy_id='qtStrategy', start='2019-01-01', end='2019-10-21'):
        self.strategy_id = strategy_id
        self.start = start
        self.end = end
        self.account = QA_User(username="admin", password='admin').new_portfolio().new_accountpro(
            account_cookie=self.strategy_id, init_cash=20000, auto_reload=False)
        self.ct = 0
        self.mongodbloader = MongoDataLoader()

    def run(self):
        # dts = pd.date_range(start=self.start, end=self.end, freq='D')
        busdaysPd = self.mongodbloader.load_trade_cal()
        busiday = pd.to_datetime(busdaysPd['cal_date'])[
            (busdaysPd['cal_date'] >= self.start) & (busdaysPd['cal_date'] <= self.end)] \
            .sort_values()
        busiday.apply(self.move)

    def move(self, item):
        self.cal_ind(item)
        self.make_decision(item)

    def make_decision(self, item):
        self.ct += 1
        print(self.ct)
        print(datetime.strftime(item, '%Y-%m-%d'))

    def cal_ind(self, item):
        pass


if __name__ == '__main__':
    Strategy(strategy_id='qtStrategy', start='2019-01-01', end='2019-12-31').run()
