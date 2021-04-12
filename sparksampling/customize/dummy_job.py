from sparksampling.core.job.base_job import BaseJob
from pyspark.sql import DataFrame


class DummyJob(BaseJob):
    def _generate(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        # imp this method for sampling_job
        return df

    def _statistics(self, df: DataFrame, *args, **kwargs) -> dict:
        # imp this method for statistics_job or evaluation_job
        return {'auc': 100}
