from service.ReportStrategy import ReportStrategy
from pandas import DataFrame
from utils import *
import pandas as pd
from datetime import datetime


class GovLastWeekAddOld2ReportStrategyImpl(ReportStrategy):
    COLUMNS = [Constant.NUMBER, Constant.BANK_NAME, Constant.CUSTOMER_NAME,
               Constant.APPLY_MONEY, Constant.CATEGORY, Constant.AUTH, Constant.OWNER, Constant.MANAGER,
               Constant.MEETING_DATE, Constant.REAPPLY, Constant.AGREE_MONEY, Constant.FINAL_ADD_MONEY, Constant.GROUP]

    def create_report(self, data: DataFrame, match: dict) -> DataFrame:
        LogUtil.info("5.4公司部-本周新增额度-政府存量新增,开始执行")
        first_filter = CommonUtil.last_week_add_blank_filter(data)
        second_filter = first_filter[first_filter[Constant.NEW].notna()]  # 非空
        # 不为变更 不为新 为江
        second_filter = second_filter[~second_filter[Constant.NEW].str.contains(Constant.NEW) &
                                      ~second_filter[Constant.NEW].str.contains(Constant.CHANGE) &
                                      second_filter[Constant.LEADER] == Constant.JIANG]
        second_filter[Constant.FINAL_ADD_MONEY] = None
        second_filter[Constant.NUMBER] = None
        if len(second_filter) == 0:
            LogUtil.info('5.4公司部-本周新增额度-政府存量新增, 筛选数据为空')
            second_filter[Constant.NUMBER] = range(1, len(second_filter) + 1)
            return second_filter.loc[:, self.COLUMNS]

        money_2022, money_2023 = CommonUtil.construct_match_data(match)
        second_filter[Constant.MEETING_DATE] = pd.to_datetime(second_filter[Constant.MEETING_DATE]).dt.strftime('%Y/%m/%d')

        result = CommonUtil.match_and_sort(second_filter, money_2022, money_2023, False)
        result[Constant.NUMBER] = range(1, len(result) + 1)

        return result.loc[:, self.COLUMNS]
