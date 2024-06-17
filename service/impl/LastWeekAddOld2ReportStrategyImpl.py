from service.ReportStrategy import ReportStrategy
from pandas import DataFrame
from utils import *
import pandas as pd
from datetime import datetime


class LastWeekAddOld2ReportStrategyImpl(ReportStrategy):
    COLUMNS = [Constant.NUMBER, Constant.BANK_NAME, Constant.CUSTOMER_NAME, Constant.CUSTOMER_MANAGER,
               Constant.APPLY_MONEY, Constant.CATEGORY, Constant.AUTH, Constant.OWNER, Constant.MANAGER,
               Constant.MEETING_DATE, Constant.REAPPLY, Constant.AGREE_MONEY, Constant.FINAL_ADD_MONEY, Constant.GROUP]

    def create_report(self, data: DataFrame, match: dict, start_date: datetime.date,
                      end_date: datetime.date) -> DataFrame:
        LogUtil.info("5.2公司部-本周新增额度-企业存量新增, start")
        first_filter = CommonUtil.last_week_add_blank_filter(data, start_date, end_date)
        second_filter = first_filter[first_filter[Constant.NEW].apply(lambda o:
                                                                      pd.notna(o) and
                                                                      isinstance(o, str) and
                                                                      Constant.NEW not in o and
                                                                      Constant.CHANGE not in o)]
        # 不为江
        second_filter = second_filter[second_filter[Constant.LEADER] != Constant.JIANG]
        second_filter[Constant.FINAL_ADD_MONEY] = None
        money_2022, money_2023 = dict(), dict()
        # 2022年数据
        data_2022 = match.get(Constant.LAST_LAST_YEAR_SHEET)
        data_2022 = data_2022[data_2022[Constant.MEETING_DATE].apply(lambda t: pd.notna(t) and isinstance(t, datetime))]
        data_2022.sort_values(by=Constant.MEETING_DATE, inplace=True)
        for index, row in data_2022.iterrows():
            money_2022[row[Constant.CUSTOMER_NAME]] = row[Constant.AGREE_MONEY]
        # 2023年数据
        data_2023 = match.get(Constant.LAST_YEAR_SHEET)
        data_2023 = data_2023[data_2023[Constant.MEETING_DATE].apply(lambda t: pd.notna(t) and isinstance(t, datetime))]
        data_2023.sort_values(by=Constant.MEETING_DATE, inplace=True)
        for index, row in data_2023.iterrows():
            money_2023[row[Constant.CUSTOMER_NAME]] = row[Constant.AGREE_MONEY]

        for index, row in second_filter.iterrows():
            customer_name = row[Constant.CUSTOMER_NAME]
            if customer_name not in money_2022.keys() and customer_name not in money_2023.keys():
                continue
            money_last = money_2023.get(customer_name) if customer_name in money_2023.keys() else money_2022.get(
                customer_name)
            money_cur = row[Constant.AGREE_MONEY]
            if isinstance(money_cur, int) and isinstance(money_last, int):
                diff = money_cur - money_last
                second_filter.loc[index, Constant.FINAL_ADD_MONEY] = diff
        second_filter = second_filter[
            second_filter[Constant.FINAL_ADD_MONEY].apply(lambda x: pd.notna(x) and isinstance(x, int) and x > 0)]
        # 权限为内
        final_report = CommonUtil.auth_in_out(second_filter, False, Constant.MEETING_DATE)
        return final_report.loc[:, self.COLUMNS]
