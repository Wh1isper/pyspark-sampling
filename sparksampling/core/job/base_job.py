from sparksampling.core import CheckLogger
from pyspark.sql import DataFrame


class BaseJob(CheckLogger):
    def __init__(self, *args, **kwargs):
        self.job_id = None

    def prepare(self, *args, **kwargs) -> dict:
        return {}

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
