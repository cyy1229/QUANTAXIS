from datetime import datetime

from QUANTAXIS import QA_User


class RandomStockStrategy:
    def __init__(self, strategy_id='RandomStockStrategy', context=None):
        self.strategy_id = strategy_id

    def init(self,context):
        self.context = context
        cookie = self.__class__.__name__ + "|" + self.strategy_id + "|" + self.context.start + "|" + self.context.end + "|" + str(
            datetime.now())
        self.acc = QA_User(username="admin", password='admin').new_portfolio(cookie).new_accountpro(
        account_cookie=self.strategy_id, init_cash=1000000, auto_reload=False)


    def on_bar(self, item):
        print(self.__class__.__name__ + ":" + str(item))
        # self.acc.receive_simpledeal(code = '000001.SZ',
        #                    trade_price,
        #                    trade_amount,
        #                    trade_towards,
        #                    trade_time,
        #                    message=None,
        #                    order_id=None,
        #                    trade_id=None,
        #                    realorder_id=None)


