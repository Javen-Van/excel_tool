from service.ReportStrategy import ReportStrategy
from pandas import DataFrame
from utils import *
import pandas as pd
from datetime import datetime


class LastWeekAddOldReportStrategyImpl(ReportStrategy):
    COLUMNS = [Constant.NUMBER, Constant.BANK_NAME, Constant.CUSTOMER_NAME,
               Constant.APPLY_MONEY, Constant.CATEGORY, Constant.AUTH,
               Constant.OWNER, Constant.MANAGER, Constant.MEETING_DATE, Constant.REAPPLY, Constant.AGREE_MONEY,
               Constant.FINAL_ADD_MONEY, Constant.GROUP]
    MAX_VALUE = 100000000000

    def create_report(self, data: DataFrame, match: dict) -> DataFrame:
        LogUtil.info("4.2上周新增客户-存量新增开始执行")
        first_filter = CommonUtil.last_week_add_blank_filter(data)
        second_filter = first_filter[first_filter[Constant.NEW].notna() & ~first_filter[Constant.NEW].str.contains(Constant.NEW)
                                     & ~first_filter[Constant.NEW].str.contains(Constant.CHANGE)]
        second_filter[Constant.FINAL_ADD_MONEY] = None
        # 整理22、23年数据
        money_2022, money_2023 = CommonUtil.construct_match_data(match)
        second_filter[Constant.MEETING_DATE] = pd.to_datetime(second_filter[Constant.MEETING_DATE]).dt.strftime('%Y/%m/%d')

        # 江组长
        gov_report = second_filter[second_filter[Constant.LEADER] == Constant.JIANG]
        gov_report = CommonUtil.match_and_sort(gov_report, money_2022, money_2023, True)
        size = len(gov_report)
        gov_report[Constant.NUMBER] = range(1, size + 1)

        # 企业类
        company_report = second_filter[second_filter[Constant.LEADER] != Constant.JIANG]
        company_report = CommonUtil.match_and_sort(company_report, money_2022, money_2023, True)
        company_report[Constant.NUMBER] = range(size + 1, len(company_report) + size + 1)
        empty_row = pd.Series([None])
        final_report = pd.concat([gov_report, empty_row, empty_row, company_report], ignore_index=True)

        return final_report.loc[:, self.COLUMNS]
