from sparksampling.core import CheckLogger
from pyspark.sql import DataFrame


class BaseJob(CheckLogger):
    def __init__(self):
        self.job_id = None

    def _generate(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        raise NotImplementedError

    def generate(self, df: DataFrame, job_id, *args, **kwargs) -> DataFrame:
        self.job_id = job_id
        self.logger.info(f"Job: {self.job_id} : Generate Sampling Job...")
        return self._generate(df, *args, **kwargs)
