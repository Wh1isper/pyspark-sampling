import copy

from sparksampling.core import CheckLogger
from pyspark.sql import DataFrame

from sparksampling.core.dataio.base_dataio import BaseDataIO


class BaseJob(CheckLogger):
    def __init__(self, *args, **kwargs):
        self.job_id = None

    def prepare(self, *args, **kwargs) -> dict:
        return kwargs

    def _generate(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        raise NotImplementedError

    def generate(self, df: DataFrame, job_id, *args, **kwargs) -> DataFrame:
        self.job_id = job_id
        self.logger.info(f"{self.__class__.__name__}: Job {self.job_id}: Generate Job...")
        return self._generate(df, *args, **kwargs)

    def _statistics(self, df: DataFrame, *args, **kwargs) -> dict:
        raise NotImplementedError

    def statistics(self, df: DataFrame, job_id, *args, **kwargs) -> dict:
        self.job_id = job_id
        self.logger.info(f"{self.__class__.__name__}: Job {self.job_id}: Running Statistics...")
        return self._statistics(df, *args, **kwargs)

    def _get_df_from_source(self, source_path, dataio) -> DataFrame:
        # 对比评估时可通过此函数获得source_path的dataframe
        return dataio.read(self.job_id, source_path)
