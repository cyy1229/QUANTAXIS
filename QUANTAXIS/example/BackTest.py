import pandas as pd
from DataFetcher import MongoDataLoader
from datetime import datetime

from QUANTAXIS.example.RandomStock import RandomStockStrategy
from QUANTAXIS.example.RandomStockAndTime import RandomStockAndTimeStrategy


class StrategyExecutor:

    def __init__(self, start='2019-01-01', end='2021-06-30'):

        self.strategy_list = []

        self.context = StrategyContext()
        self.context.ct = 0
        self.context.mongodb_loader = MongoDataLoader()
        self.mongodb_loader = self.context.mongodb_loader
        self.context.start = start
        self.context.end = end

    def execute(self):
        # dts = pd.date_range(start=self.start, end=self.end, freq='D')
        busdaysPd = self.mongodb_loader.load_trade_cal()
        busidaySe = pd.to_datetime(busdaysPd['cal_date'])
        busiday =busidaySe[(busidaySe >= self.context.start) & (busidaySe <= self.context.end)].sort_values()
        busiday.apply(self.forward)

    def forward(self, item):
        self.context.ct += 1
        self.context_cal_ind(item)

        self.context._stock_day_df = self.mongodb_loader.load_stock_day()
        for st in self.strategy_list:
            st.on_bar(item)

    def context_cal_ind(self, item):
        pass

    def addStrategy(self, item):
        item.init(self.context)
        self.strategy_list.append(item)
        return self

class StrategyContext:
    def __init__(self):
        pass

if __name__ == '__main__':
    strategy = StrategyExecutor(start='2019-01-01', end='2019-12-31')
    strategy.addStrategy(RandomStockAndTimeStrategy())
    strategy.addStrategy(RandomStockStrategy())
    strategy.execute()


