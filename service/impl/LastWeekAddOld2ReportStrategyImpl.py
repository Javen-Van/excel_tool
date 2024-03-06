from service.ReportStrategy import ReportStrategy
from pandas import DataFrame
from utils import *


class LastWeekAddOld2ReportStrategyImpl(ReportStrategy):
    COLUMNS = [Constant.NUMBER, Constant.BANK_NAME, Constant.CUSTOMER_NAME, Constant.CUSTOMER_MANAGER,
               Constant.APPLY_MONEY, Constant.CATEGORY, Constant.AUTH, Constant.OWNER, Constant.MANAGER,
               Constant.MEETING_DATE, Constant.REAPPLY, Constant.AGREE_MONEY, Constant.FINAL_ADD_MONEY, Constant.GROUP]

    def create_report(self, data: DataFrame, match: dict) -> DataFrame:
        LogUtil.info("5.2公司部-本周新增额度-企业存量新增开始执行")
        first_filter = CommonUtil.last_week_add_blank_filter(data)
        second_filter = first_filter[first_filter[Constant.NEW].notna()]  # 非空
        # 不为变更 不为新 不为江
        second_filter = second_filter[~second_filter[Constant.NEW].str.contains(Constant.NEW) &
                                      ~second_filter[Constant.NEW].str.contains(Constant.CHANGE) &
                                      second_filter[Constant.LEADER] != Constant.JIANG]
        second_filter[Constant.FINAL_ADD_MONEY] = None
        money_2022, money_2023 = CommonUtil.construct_match_data(match)
        result = CommonUtil.match_and_sort(second_filter, money_2022, money_2023, False)
        # 整理权限内外
        final_report = CommonUtil.auth_in_out(result, False, Constant.MEETING_DATE)
        return final_report.loc[:, self.COLUMNS]
