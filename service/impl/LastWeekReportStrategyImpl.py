from service.ReportStrategy import ReportStrategy
from pandas import DataFrame
from utils import *
import pandas as pd


class LastWeekReportStrategyImpl(ReportStrategy):
    COLUMNS = [Constant.NUMBER, Constant.BANK_NAME, Constant.CUSTOMER_NAME, Constant.APPLY_MONEY, Constant.CATEGORY,
               Constant.NEW, Constant.OWNER, Constant.MANAGER, Constant.MEETING_DATE, Constant.LEADER,
               Constant.AGREE_MONEY, Constant.GROUP]
    MAX_VALUE = 100000000000

    def create_report(self, data: DataFrame, match: dict) -> DataFrame:
        LogUtil.info("上周审批项目明细开始执行")
        first_filter = CommonUtil.last_week_filter(data)
        first_filter[Constant.TEMP] = None
        for index, row in first_filter.iterrows():
            agree_result, auth = row[Constant.AGREE_RESULT], row[Constant.AUTH]
            first_filter.loc[index, Constant.TEMP] = self.MAX_VALUE if isinstance(row[Constant.AGREE_MONEY], str) else row[Constant.AGREE_MONEY]
            if not isinstance(agree_result, str):
                if Constant.OUT.__eq__(auth):
                    first_filter.loc[index, Constant.AGREE_MONEY] = '总行审批中'
                    first_filter.loc[index, Constant.TEMP] = self.MAX_VALUE
            else:
                if Constant.AGREE not in agree_result:
                    first_filter.loc[index, Constant.AGREE_MONEY] = agree_result
                    first_filter.loc[index, Constant.TEMP] = self.MAX_VALUE

        # 权限为内
        in_report = first_filter.loc[first_filter[Constant.AUTH] == Constant.IN]
        in_report.sort_values(by=Constant.MEETING_DATE, inplace=True)  # 按上会日期
        in_report.sort_values(by=Constant.TEMP, kind='mergesort', inplace=True)
        size = len(in_report)
        in_report.insert(0, Constant.NUMBER, range(1, size + 1))
        in_report[Constant.MEETING_DATE] = pd.to_datetime(in_report[Constant.MEETING_DATE]).dt.strftime('%Y/%m/%d')

        # 权限为外
        out_report = first_filter.loc[first_filter[Constant.AUTH] == Constant.OUT]
        out_report.sort_values(by=Constant.MEETING_DATE, inplace=True)
        out_report.sort_values(by=Constant.TEMP, kind='mergesort', inplace=True)
        out_report.insert(0, Constant.NUMBER, range(size + 1, len(out_report) + size + 1))
        out_report[Constant.MEETING_DATE] = pd.to_datetime(out_report[Constant.MEETING_DATE]).dt.strftime('%Y/%m/%d')

        empty_row = pd.Series([None])
        final_report = pd.concat([in_report, empty_row, empty_row, out_report], ignore_index=True)
        final_report = final_report.loc[:, self.COLUMNS]
        final_report.rename(columns={Constant.BANK_NAME: Constant.BANK_NAME_SIMPLE, Constant.GROUP: Constant.NOTE,
                                     Constant.NEW: Constant.NEW_GREAT},
                            inplace=True)  # 列名更改
        return final_report
