from datetime import datetime

from QUANTAXIS import QA_User


class RandomStockStrategy:
    def __init__(self, strategy_id='RandomStockStrategy', context=None):
        self.strategy_id = strategy_id
        # 生成cookie
        cookie = self.__class__.__name__ + "|" + strategy_id + "|" + context.start + "|" + context.end + "|" + str(
            datetime.now())
        self.account = QA_User(username="admin", password='admin').new_portfolio(cookie).new_accountpro(
            account_cookie=self.strategy_id, init_cash=1000000, auto_reload=False)

    def on_bar(self, item):
        print(self.__class__.__name__ + ":" + str(item))
