# import pandas, numpy
import datetime

from service.ReportFactory import ReportFactory
from service.impl import *
from utils import CommonUtil, LogUtil
import tkinter as tk
import tkinter.messagebox
import pandas as pd

pd.options.mode.chained_assignment = None


def create_window():
    window = tk.Tk()
    window.title("文件转换工具")
    # window.geometry("500x300")
    tk.Label(window, text='基础台账: ').grid(row=0, column=0)
    tk.Label(window, text='匹配文件: ').grid(row=1, column=0)
    tk.Label(window, text='保存目录: ').grid(row=2, column=0)
    tk.Label(window, text='开始日期: ').grid(row=3, column=0)
    tk.Label(window, text='结束日期: ').grid(row=4, column=0)

    entry1 = tk.Entry(window, width=40)
    entry1.grid(row=0, column=1)
    entry2 = tk.Entry(window, width=40)
    entry2.grid(row=1, column=1)
    entry3 = tk.Entry(window, width=40)
    entry3.grid(row=2, column=1)
    entry4 = tk.Entry(window, width=40)
    entry4.grid(row=3, column=1)
    entry5 = tk.Entry(window, width=40)
    entry5.grid(row=4, column=1)

    def show_function():
        file_dir = entry1.get()
        match_dir = entry2.get()
        save_dir = entry3.get()
        start_date = datetime.datetime.strptime(entry4.get(), '%Y/%m/%d').date()
        end_date = datetime.datetime.strptime(entry5.get(), '%Y/%m/%d').date()

        error = False
        try:
            start_task(file_dir, match_dir, save_dir, start_date, end_date)
        except:
            error = True
        if error:
            tkinter.messagebox.showwarning("提示", "解析失败，请联系开发人员")
        else:
            tkinter.messagebox.showinfo("提示", "解析成功，请前往桌面result文件夹查看")

    start_button = tk.Button(window, text="开始解析", width=10, height=2, command=show_function)
    start_button.grid(row=6, column=1)
    window.mainloop()


def start_task(file_dir: str, match_dir: str, save_dir: str, start_date: datetime.date, end_date: datetime.date):
    LogUtil.info('基础台账文件: {0}', file_dir)
    LogUtil.info('匹配文件: {0}', match_dir)
    LogUtil.info('保存目录: {0}', save_dir)
    data = CommonUtil.read_excel(file_dir)
    match_table = CommonUtil.read_match_excel(match_dir)
    strategy_list = {
        '1.2分行在审项目表-新客户': InProcessNewReportStrategyImpl(),
        '1.1分行在审项目表-存量客户': InProcessOldReportStrategyImpl(),
        '2.分行在审重大项目表': InProcessGreatReportStrategyImpl(),
        '4.1上周审批项目明细': LastWeekReportStrategyImpl(),
        '4.2上周新增额度-新客户': LastWeekAddReportStrategyImpl(),
        '4.2上周新增客户-存量新增': LastWeekAddOldReportStrategyImpl(),
        '5.1公司部-本周新增额度-企业新客户': LastWeekAddAuthReportStrategyImpl(),
        '5.2公司部-本周新增额度-企业存量新增': LastWeekAddOld2ReportStrategyImpl(),
        '5.3公司部-本周新增额度-政府新客户': GovLastWeekAddAuthReportStrategyImpl(),
        '5.4公司部-本周新增额度-政府存量新增': GovLastWeekAddOld2ReportStrategyImpl(),
    }
    for name, strategy in strategy_list.items():
        factory = ReportFactory(strategy)
        factory.execute(data, match_table, save_dir + '/' + name, start_date, end_date)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    create_window()
