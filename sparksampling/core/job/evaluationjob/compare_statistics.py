from sparksampling.core.engine.statistics_engine import StatisticsEngine
from sparksampling.core.job.base_job import BaseJob
from pyspark.sql import DataFrame

from sparksampling.customize.custom_config import compare_evaluation_code


class CompareStatisticsJob(BaseJob):
    type_map = {
        'source_path': str
    }

    def __init__(self, source_path=None, *args, **kwargs):
        super(CompareStatisticsJob, self).__init__(*args, **kwargs)
        self.source_path = source_path
        self.check_type()

    def _statistics(self, df: DataFrame, *args, **kwargs) -> dict:
        source_statistics = kwargs.get('source_statistics')
        statistics = StatisticsEngine.job_map.get(compare_evaluation_code).statistics(df, *args, **kwargs)
        return {
            's': statistics,
            'ss': source_statistics,
        }
