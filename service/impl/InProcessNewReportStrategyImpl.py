import datetime

from pandas import DataFrame
from service.ReportStrategy import ReportStrategy
from utils import *


# 在审新客户报表策略类
class InProcessNewReportStrategyImpl(ReportStrategy):
    COLUMNS = [Constant.NUMBER, Constant.BANK_NAME, Constant.CUSTOMER_NAME, Constant.APPLY_MONEY, Constant.CATEGORY,
               Constant.AUTH, Constant.OWNER, Constant.MANAGER, Constant.REGISTER_DATE, Constant.LEADER, Constant.GROUP]

    def create_report(self, data: DataFrame, match: dict, start_date: datetime.date, end_date: datetime.date) -> DataFrame:
        LogUtil.info("1.2分行在审项目表-新客户, start")
        first_filter = CommonUtil.in_process_filter(data)  # 初筛
        second_filter = first_filter[first_filter[Constant.NEW].str.contains(Constant.NEW)]
        second_filter.sort_values(by=Constant.REGISTER_DATE, inplace=True)  # 按登记日期
        final_report = CommonUtil.auth_in_out(second_filter, True, Constant.REGISTER_DATE)
        final_report = final_report.loc[:, self.COLUMNS]
        final_report.rename(columns={Constant.BANK_NAME: Constant.BANK_NAME_SIMPLE,
                                     Constant.GROUP: Constant.NOTE,
                                     Constant.OWNER: Constant.OWNER_SIMPLE},
                            inplace=True)  # 列名更改
        return final_report
