from service.ReportStrategy import ReportStrategy
from pandas import DataFrame
from utils import *
import pandas as pd
from utils.CommonUtil import leader_dict


class InProcessGreatReportStrategyImpl(ReportStrategy):
    COLUMNS = [Constant.BANK_NAME, Constant.CUSTOMER_NAME, Constant.APPLY_MONEY, Constant.CATEGORY, Constant.NEW,
               Constant.AUTH, Constant.OWNER, Constant.MANAGER, Constant.REGISTER_DATE, Constant.LEADER, Constant.GROUP]

    CATEGORY_LIST = ['并购贷款', '并购银团', '固定资产贷款', '固定资产贷款+综合授信', '城市更新贷款', '园区建设贷款',
                     '定增配资', '股票质押融资', '可转债配资', '可转债投资', '战略配售', '结构化融资', '开发贷',
                     '法人按揭', '经营性物业贷', '隐债置换']

    def create_report(self, data: DataFrame, match: dict):
        LogUtil.info("在审重大项目报表开始制作")
        first_filter = CommonUtil.in_process_filter(data)  # 初筛
        report = first_filter[first_filter[Constant.CATEGORY].apply(lambda category: category in self.CATEGORY_LIST)]
        report = report.loc[:, self.COLUMNS]
        report.sort_values(by=Constant.REGISTER_DATE, inplace=True)  # 按登记日期
        report[Constant.REGISTER_DATE] = pd.to_datetime(report[Constant.REGISTER_DATE]).dt.strftime('%Y/%m/%d')
        report.sort_values(by=Constant.LEADER, key=lambda s: s.map(leader_dict), kind='mergesort', inplace=True)
        size = len(report)
        report.insert(0, Constant.NUMBER, range(1, size + 1))
        report.rename(columns={Constant.BANK_NAME: Constant.BANK_NAME_SIMPLE,
                               Constant.GROUP: Constant.NOTE,
                               Constant.NEW: Constant.NEW_GREAT},
                      inplace=True)  # 列名更改

        return report
