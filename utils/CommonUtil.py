import datetime
import math

from pandas import DataFrame
import pandas as pd
from utils import LogUtil, Constant

# 组长排序
leader_dict = {
    "闻倩倩": 1,
    "江志锋": 2,
    "曹光艳": 3,
    "薛怡": 4,
    "周峰": 5
}

category_list = ['ABS', '分离式保函', '同业授信']


def read_excel(file_dir: str) -> DataFrame:
    LogUtil.info("开始读取excel文件,文件路径:{0}", file_dir)
    return pd.read_excel(file_dir, sheet_name=Constant.SHEET_NAME, usecols="A:AI")


def read_match_excel(file_dir: str) -> dict:
    LogUtil.info("开始读取匹配excel文件,文件路径:{0}", file_dir)
    return pd.read_excel(file_dir, sheet_name=[Constant.LAST_LAST_YEAR_SHEET, Constant.LAST_YEAR_SHEET])


def to_excel(data: DataFrame, save_dir: str):
    data.to_excel('{0}.xlsx'.format(save_dir), index=False)


def in_process_filter(data: DataFrame) -> DataFrame:
    # 上会日期为空 and '新'不为空 and '登记日期'不为空
    var = data[data[Constant.MEETING_DATE].isna() & data[Constant.REGISTER_DATE].notna() & data[Constant.NEW].notna()]
    # 登记日期不为文字 and '新'不为变更
    return var[~var[Constant.NEW].str.contains(Constant.CHANGE) & var[Constant.REGISTER_DATE].apply(lambda time: isinstance(time, datetime.datetime))]


# 按上会日期或总行批复日期筛选
def last_week_filter(data: DataFrame) -> DataFrame:
    today = datetime.date.today()
    # TODO: 待更正
    # today = datetime.date(2024, 1, 19)
    start_of_week = today - datetime.timedelta(days=today.weekday())
    end_of_week = today + datetime.timedelta(days=6 - today.weekday())
    final_index = []
    for index, row in data.iterrows():
        meeting_date = row[Constant.MEETING_DATE]
        agree_date = row[Constant.AGREE_DATE]
        special_instruction = row[Constant.SPECIAL_INSTRUCTION]
        date_list = analyze_date(special_instruction)
        # 上会日期是当周
        if pd.notna(meeting_date) and isinstance(meeting_date, datetime.datetime) and (start_of_week <= meeting_date.date() <= end_of_week):
            final_index.append(index)
        elif pd.notna(agree_date) and isinstance(agree_date, datetime.datetime) and (start_of_week <= agree_date.date() <= end_of_week):
            final_index.append(index)  # 总行批复日期在当周
        elif len(date_list) != 0 and (start_of_week <= date_list[-1] <= end_of_week):
            final_index.append(index)  # 再上会日期在当周
    return data.loc[final_index]


def analyze_date(data) -> list:
    year = datetime.datetime.now().year  # 当前所在年份
    date_list = []  # 可能存在多个再上会，先暂存，排序后取最近的一条记录
    if pd.notna(data) and isinstance(data, str):
        tmp_list = data.split('，')
        for tmp in tmp_list:
            idx = tmp.find('再上会')
            if idx != -1:
                time_list = tmp[:idx].split('.')
                date_list.append(datetime.date(year, int(time_list[0]), int(time_list[1])))
    date_list.sort()
    return date_list


def last_week_add_blank_filter(data: DataFrame) -> DataFrame:
    var = last_week_filter(data)
    var = var[var[Constant.LEADER].notna() & var[Constant.CATEGORY].notna()]
    return var[var[Constant.AGREE_RESULT].apply(lambda x: pd.isna(x) or Constant.AGREE in str(x)) &
               var[Constant.CATEGORY].apply(lambda x: x not in category_list and Constant.BOND not in str(x))]


# 按权限聚类, 中间隔两行
def auth_in_out(data: DataFrame, sort_by_leader: bool, column_name: str) -> DataFrame:
    # 权限为内
    in_report = data[data[Constant.AUTH] == Constant.IN]
    # 按组长排序
    if sort_by_leader:
        in_report.sort_values(by=Constant.LEADER, key=lambda s: s.map(leader_dict), kind='mergesort', inplace=True)
    size = len(in_report)
    # in_report.insert(0, Constant.NUMBER, range(1, size + 1))
    in_report[Constant.NUMBER] = range(1, size + 1)
    in_report[column_name] = pd.to_datetime(in_report[column_name]).dt.strftime('%Y/%m/%d')

    # 权限为外
    out_report = data[data[Constant.AUTH] == Constant.OUT]
    if sort_by_leader:
        out_report.sort_values(by=Constant.LEADER, key=lambda s: s.map(leader_dict), kind='mergesort', inplace=True)
    # out_report.insert(0, Constant.NUMBER, range(size + 1, len(out_report) + size + 1))
    out_report[Constant.NUMBER] = range(size + 1, len(out_report) + size + 1)
    out_report[column_name] = pd.to_datetime(out_report[column_name]).dt.strftime('%Y/%m/%d')

    empty_row = pd.Series([None])
    return pd.concat([in_report, empty_row, empty_row, out_report], ignore_index=True)


# 匹配并排序
def match_and_sort(data: DataFrame, money_2022: dict, money_2023: dict, sort_by_bank: bool) -> DataFrame:
    # 需要特殊处理不做匹配的授信品种
    special_category_list = ['法人按揭', '经营性物业贷', '开发贷', '园区建设贷款', '城市更新贷款', '隐债置换',
                             '股票质押融资', '结构化融资', '定增配资', '可转债配资', '可转债投资', '战略配售']
    # 各支行新增额度统计归类
    bank_add_sum = dict()
    for index, row in data.iterrows():
        customer_name = row[Constant.CUSTOMER_NAME]  # 客户名称
        bank_name = row[Constant.BANK_NAME]  # 支行名称
        category = row[Constant.CATEGORY]  # 授信品种
        money_cur = row[Constant.AGREE_MONEY]  # 批复金额
        note = row[Constant.GROUP]  # 备注
        # 2022/2023年中未匹配上的，取空值，跳过
        if customer_name not in money_2022.keys() and customer_name not in money_2023.keys():
            continue
        money_last = money_2023.get(customer_name) if customer_name in money_2023.keys() else money_2022.get(customer_name)
        # 特殊授信品种处理，直接取批复金额
        if category in special_category_list or '固定资产' in category or '并购贷款' in category:
            data.loc[index, Constant.FINAL_ADD_MONEY] = money_cur
            money_last = 0
            note_list = [note, '新增专项授信'] if pd.notnull(note) and isinstance(note, str) else ['新增专项授信']
            data.loc[index, Constant.GROUP] = '，'.join(note_list)
        if isinstance(money_cur, int) and isinstance(money_last, int):
            diff = money_cur - money_last
            data.loc[index, Constant.FINAL_ADD_MONEY] = diff
            if isinstance(bank_name, str) and diff > 0:
                cur = bank_add_sum.get(bank_name, 0)
                bank_add_sum[bank_name] = cur + diff
    result = data[data[Constant.FINAL_ADD_MONEY].apply(lambda x: pd.notna(x) and isinstance(x, int) and x > 0) |
                  data[Constant.CATEGORY].apply(lambda x: x in special_category_list or '固定资产' in x or '并购贷款' in x)]
    if sort_by_bank:
        result.sort_values(by=Constant.BANK_NAME, ascending=False, key=lambda x: x.map(bank_add_sum), inplace=True)
    return result


# 整理22、23年数据
def construct_match_data(match: dict):
    money_2022, money_2023 = dict(), dict()
    # 2022年数据
    data_2022 = match.get(Constant.LAST_LAST_YEAR_SHEET)
    data_2022 = data_2022[data_2022[Constant.MEETING_DATE].apply(lambda t: pd.notna(t) and isinstance(t, datetime.datetime))]
    data_2022.sort_values(by=Constant.MEETING_DATE, inplace=True)
    for index, row in data_2022.iterrows():
        money_2022[row[Constant.CUSTOMER_NAME]] = row[Constant.AGREE_MONEY]
    # 2023年数据
    data_2023 = match.get(Constant.LAST_YEAR_SHEET)
    data_2023 = data_2023[data_2023[Constant.MEETING_DATE].apply(lambda t: pd.notna(t) and isinstance(t, datetime.datetime))]
    data_2023.sort_values(by=Constant.MEETING_DATE, inplace=True)
    for index, row in data_2023.iterrows():
        money_2023[row[Constant.CUSTOMER_NAME]] = row[Constant.AGREE_MONEY]
    return money_2022, money_2023
