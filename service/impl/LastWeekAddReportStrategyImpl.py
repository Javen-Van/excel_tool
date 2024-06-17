import datetime

from service.ReportStrategy import ReportStrategy
from pandas import DataFrame
from utils import *
import pandas as pd


def sort(data: DataFrame):
    bank_agree_sum = dict()
    for index, row in data.iterrows():
        bank_name, money = row[Constant.BANK_NAME], row[Constant.AGREE_MONEY]
        if isinstance(bank_name, str) and isinstance(money, int):
            cur = bank_agree_sum.get(bank_name, 0)
            bank_agree_sum[bank_name] = cur + money
    data.sort_values(by=Constant.BANK_NAME, ascending=False, key=lambda x: x.map(bank_agree_sum), inplace=True)
    return data


class LastWeekAddReportStrategyImpl(ReportStrategy):
    COLUMNS = [Constant.NUMBER, Constant.BANK_NAME, Constant.CUSTOMER_NAME, Constant.APPLY_MONEY, Constant.CATEGORY,
               Constant.AUTH, Constant.OWNER, Constant.MANAGER, Constant.MEETING_DATE, Constant.REAPPLY,
               Constant.AGREE_MONEY, Constant.GROUP, Constant.INDUSTRY]

    def create_report(self, data: DataFrame, match: dict, start_date: datetime.date,
                      end_date: datetime.date) -> DataFrame:
        LogUtil.info("4.2上周新增额度-新客户, start")
        first_filter = CommonUtil.last_week_add_blank_filter(data, start_date, end_date)
        # 包含 新
        second_filter = first_filter[first_filter[Constant.NEW].apply(lambda o:
                                                                      pd.notna(o) and
                                                                      isinstance(o, str) and
                                                                      Constant.NEW in o)]
        second_filter[Constant.MEETING_DATE] = pd.to_datetime(second_filter[Constant.MEETING_DATE]).dt.strftime(
            '%Y/%m/%d')

        # 江组长
        gov_report = second_filter[second_filter[Constant.LEADER] == Constant.JIANG]
        gov_report = sort(gov_report)
        size = len(gov_report)
        gov_report[Constant.NUMBER] = range(1, size + 1)
        # 企业类
        company_report = second_filter[second_filter[Constant.LEADER] != Constant.JIANG]
        company_report = sort(company_report)
        company_report[Constant.NUMBER] = range(size + 1, len(company_report) + size + 1)
        empty_row = pd.Series([None])
        final_report = pd.concat([gov_report, empty_row, empty_row, company_report], ignore_index=True)
        return final_report.loc[:, self.COLUMNS]
