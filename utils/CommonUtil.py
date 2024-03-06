import datetime
from pandas import DataFrame
import pandas as pd
from utils import LogUtil, Constant

# 组长排序
leader_dict = {
    "闻倩倩": 1,
    "江志锋": 2,
    "曹光艳": 3,
    "薛怡": 4
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
    return var[~var[Constant.NEW].str.contains(Constant.CHANGE) &
               var[Constant.REGISTER_DATE].apply(lambda time: isinstance(time, datetime.datetime))]


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
        # 上会日期是当周
        if (pd.notna(meeting_date) and isinstance(meeting_date, datetime.datetime) and
                (start_of_week <= meeting_date.date() <= end_of_week)):
            final_index.append(index)
        else:
            if (pd.notna(agree_date) and isinstance(agree_date, datetime.datetime)
                    and (start_of_week <= agree_date.date() <= end_of_week)):
                final_index.append(index)  # 总行批复日期在当周
    return data.loc[final_index]


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
