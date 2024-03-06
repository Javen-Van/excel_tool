from service.ReportStrategy import ReportStrategy
from pandas import DataFrame
from utils import *
import pandas as pd


class GovLastWeekAddAuthReportStrategyImpl(ReportStrategy):
    COLUMNS = [Constant.NUMBER, Constant.BANK_NAME, Constant.CUSTOMER_NAME,
               Constant.APPLY_MONEY, Constant.CATEGORY, Constant.AUTH, Constant.OWNER, Constant.MANAGER,
               Constant.MEETING_DATE, Constant.REAPPLY, Constant.AGREE_MONEY, Constant.GROUP]

    def create_report(self, data: DataFrame, match_table: dict) -> DataFrame:
        LogUtil.info("上周新增额度审批明细(分权限)开始执行")
        first_filter = CommonUtil.last_week_add_blank_filter(data)
        first_filter[Constant.NUMBER] = None
        if len(first_filter) == 0:
            return first_filter.loc[:, self.COLUMNS]
        second_filter = first_filter[first_filter[Constant.LEADER] == Constant.JIANG]
        second_filter = second_filter[second_filter[Constant.NEW].apply(lambda s: pd.notna(s) and Constant.NEW in str(s))]
        second_filter[Constant.NUMBER] = range(1, len(second_filter) + 1)
        second_filter[Constant.MEETING_DATE] = pd.to_datetime(second_filter[Constant.MEETING_DATE]).dt.strftime('%Y/%m/%d')
        return second_filter.loc[:, self.COLUMNS]
