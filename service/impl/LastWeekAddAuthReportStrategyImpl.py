import datetime

from service.ReportStrategy import ReportStrategy
from pandas import DataFrame
from utils import *
import pandas as pd


class LastWeekAddAuthReportStrategyImpl(ReportStrategy):
    COLUMNS = [Constant.NUMBER, Constant.BANK_NAME, Constant.CUSTOMER_NAME, Constant.CUSTOMER_MANAGER,
               Constant.APPLY_MONEY, Constant.CATEGORY, Constant.AUTH, Constant.OWNER, Constant.MANAGER,
               Constant.MEETING_DATE, Constant.REAPPLY, Constant.AGREE_MONEY, Constant.GROUP]

    def create_report(self, data: DataFrame, match_table: dict, start_date: datetime.date, end_date: datetime.date) -> DataFrame:
        LogUtil.info("5.1公司部-本周新增额度-企业新客户, start")
        first_filter = CommonUtil.last_week_add_blank_filter(data, start_date, end_date)
        first_filter[Constant.NUMBER] = None
        if len(first_filter) == 0:
            return first_filter.loc[:, self.COLUMNS]
        first_filter = first_filter[first_filter[Constant.LEADER] != Constant.JIANG]
        second_filter = first_filter[first_filter[Constant.NEW].apply(lambda s: pd.notna(s) and Constant.NEW in str(s))]
        final_report = CommonUtil.auth_in_out(second_filter, False, Constant.MEETING_DATE)
        return final_report.loc[:, self.COLUMNS]
