import datetime
from abc import abstractmethod, ABCMeta
from pandas import DataFrame


class ReportStrategy(metaclass=ABCMeta):

    @abstractmethod
    def create_report(self, data: DataFrame, match_table: dict, start_date: datetime.date, end_date: datetime.date) -> DataFrame:
        pass
