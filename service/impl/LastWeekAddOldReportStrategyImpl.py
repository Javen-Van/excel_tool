from service.ReportStrategy import ReportStrategy
from pandas import DataFrame
from utils import *
import pandas as pd
import datetime


def sort(data: DataFrame, money_2022, money_2023):
    bank_add_sum = dict()
    for index, row in data.iterrows():
        customer_name = row[Constant.CUSTOMER_NAME]
        bank_name = row[Constant.BANK_NAME]
        if customer_name not in money_2022.keys() and customer_name not in money_2023.keys():
            continue
        money_last = money_2023.get(customer_name) if customer_name in money_2023.keys() else money_2022.get(
            customer_name)
        money_cur = row[Constant.AGREE_MONEY]
        if isinstance(money_cur, int) and isinstance(money_last, int):
            diff = money_cur - money_last
            data.loc[index, Constant.FINAL_ADD_MONEY] = diff
            if isinstance(bank_name, str) and diff > 0:
                cur = bank_add_sum.get(bank_name, 0)
                bank_add_sum[bank_name] = cur + diff
    second_filter = data[
        data[Constant.FINAL_ADD_MONEY].apply(lambda x: pd.notna(x) and isinstance(x, int) and x > 0)]
    second_filter.sort_values(by=Constant.BANK_NAME, ascending=False, key=lambda x: x.map(bank_add_sum),
                              inplace=True)
    return second_filter


class LastWeekAddOldReportStrategyImpl(ReportStrategy):
    COLUMNS = [Constant.NUMBER, Constant.BANK_NAME, Constant.CUSTOMER_NAME,
               Constant.APPLY_MONEY, Constant.CATEGORY, Constant.AUTH,
               Constant.OWNER, Constant.MANAGER, Constant.MEETING_DATE, Constant.REAPPLY, Constant.AGREE_MONEY,
               Constant.FINAL_ADD_MONEY, Constant.GROUP, Constant.INDUSTRY]
    MAX_VALUE = 100000000000

    def create_report(self, data: DataFrame, match: dict) -> DataFrame:
        LogUtil.info("上周新增额度审批明细(存量)开始执行")
        first_filter = CommonUtil.last_week_add_blank_filter(data)
        second_filter = first_filter[
            first_filter[Constant.NEW].notna() & ~first_filter[Constant.NEW].str.contains(Constant.NEW) & ~first_filter[
                Constant.NEW].str.contains(Constant.CHANGE)]
        second_filter[Constant.FINAL_ADD_MONEY] = None
        money_2022, money_2023 = dict(), dict()
        # 2022年数据
        data_2022 = match.get(Constant.LAST_LAST_YEAR_SHEET)
        data_2022 = data_2022[data_2022[Constant.MEETING_DATE].notna() & data_2022[Constant.MEETING_DATE].apply(
            lambda t: isinstance(t, datetime.datetime))]
        data_2022.sort_values(by=Constant.MEETING_DATE, inplace=True)
        for index, row in data_2022.iterrows():
            money_2022[row[Constant.CUSTOMER_NAME]] = row[Constant.AGREE_MONEY]
        # 2022年数据
        data_2023 = match.get(Constant.LAST_YEAR_SHEET)
        data_2023 = data_2023[data_2023[Constant.MEETING_DATE].notna() & data_2023[Constant.MEETING_DATE].apply(
            lambda t: isinstance(t, datetime.datetime))]
        data_2023.sort_values(by=Constant.MEETING_DATE, inplace=True)
        for index, row in data_2023.iterrows():
            money_2023[row[Constant.CUSTOMER_NAME]] = row[Constant.AGREE_MONEY]

        second_filter[Constant.MEETING_DATE] = pd.to_datetime(second_filter[Constant.MEETING_DATE]).dt.strftime(
            '%Y/%m/%d')

        # 江组长
        gov_report = second_filter[second_filter[Constant.LEADER] == Constant.JIANG]
        gov_report = sort(gov_report, money_2022, money_2023)
        size = len(gov_report)
        gov_report[Constant.NUMBER] = range(1, size + 1)

        # 企业类
        company_report = second_filter[second_filter[Constant.LEADER] != Constant.JIANG]
        company_report = sort(company_report, money_2022, money_2023)
        company_report[Constant.NUMBER] = range(size + 1, len(company_report) + size + 1)
        empty_row = pd.Series([None])
        final_report = pd.concat([gov_report, empty_row, empty_row, company_report], ignore_index=True)

        return final_report.loc[:, self.COLUMNS]
