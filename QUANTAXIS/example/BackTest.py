import pandas as pd
from DataFetcher import MongoDataLoader
from datetime import datetime

from QUANTAXIS.example.RandomStock import RandomStockStrategy
from QUANTAXIS.example.RandomStockAndTime import RandomStockAndTimeStrategy


class StrategyExecutor:

    def __init__(self, start='2019-01-01', end='2019-10-21'):
        self.start = start
        self.end = end

        self.ct = 0
        self.mongodb_loader = MongoDataLoader()
        self.strategy_list = []

    def run(self):
        # dts = pd.date_range(start=self.start, end=self.end, freq='D')
        busdaysPd = self.mongodb_loader.load_trade_cal()
        busidaySe = pd.to_datetime(busdaysPd['cal_date'])
        busiday =busidaySe[(busidaySe >= self.start) & (busidaySe <= self.end)].sort_values()
        busiday.apply(self.forward)

    def forward(self, item):
        self.ct += 1
        # print(self.ct)
        self.cal_ind(item)
        # self.make_decision(item)
        for st in self.strategy_list:
            st.on_bar(item)

    # def make_decision(self, item):
    #     print(datetime.strftime(item, '%Y-%m-%d'))

    def cal_ind(self, item):
        pass

    def addStrategy(self, item):
        self.strategy_list.append(item)
        return self


if __name__ == '__main__':
    strategy = StrategyExecutor(start='2019-01-01', end='2019-12-31')
    strategy.addStrategy(RandomStockAndTimeStrategy(context=strategy))
    strategy.addStrategy(RandomStockStrategy(context=strategy))
    strategy.run()


