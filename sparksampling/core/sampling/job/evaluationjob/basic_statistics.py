from sparksampling.core.sampling.job.base_job import BaseJob
from pyspark.sql import DataFrame


class BasicStatisticsJob(BaseJob):
    def __init__(self, *args, **kwargs):
        super(BasicStatisticsJob, self).__init__()

    def _statistics(self, df: DataFrame, *args, **kwargs) -> dict:
        self.logger.info("Generating description...")
        return df.describe().toPandas().to_dict('records')
