import datetime

from pandas import DataFrame
from service.ReportStrategy import ReportStrategy
from utils import CommonUtil, LogUtil


class ReportFactory:

    def __init__(self, strategy: ReportStrategy):
        self.strategy = strategy

    def execute(self, data: DataFrame, match_table: dict, save_dir: str, start_date: datetime.date, end_date: datetime.date):
        try:
            result = self.strategy.create_report(data, match_table, start_date, end_date)
            CommonUtil.to_excel(result, save_dir)
        except Exception as e:
            print(e)
            LogUtil.error('{0}执行失败', save_dir)
