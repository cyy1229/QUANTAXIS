from QUANTAXIS import QA_Setting

QASETTING = QA_Setting()
DATABASE = QASETTING.client.quantaxis
from loguru import logger
import pandas as pd


class TushareDataLoader():
    def __init__(self):
        self.stock_day_df = pd.DataFrame([item for item in DATABASE.tushare_stock_day.find({})])
        logger.info("get tushare_stock_day all count = {}", self.stock_day_df.shape[0])

    def query_all_stock_day(self):
        # DATABASE.
        pass


if __name__ == '__main__':
    TushareDataLoader()
