from sparksampling.core.engine.statistics_engine import StatisticsEngine
from sparksampling.core.job.base_job import BaseJob
from pyspark.sql import DataFrame

from sparksampling.customize.custom_config import compare_evaluation_code


class CompareEvaluationJob(BaseJob):
    type_map = {
        'source_path': str
    }

    def __init__(self, source_path=None, *args, **kwargs):
        super(CompareEvaluationJob, self).__init__(*args, **kwargs)
        self.source_path = source_path
        self.check_type()

    def _statistics(self, df: DataFrame, *args, **kwargs) -> dict:
        source_statistics = kwargs.get('source_statistics')
        statistics_job_class = StatisticsEngine.job_map.get(compare_evaluation_code)
        statistics = statistics_job_class().statistics(df, self.job_id, *args, **kwargs)
        return {
            's': statistics,
            'ss': source_statistics,
        }

    def prepare(self, *args, **kwargs) -> dict:
        self.logger.info(f"{self.__class__.__name__}: Prepare for job...job_id: {self.job_id}")
        file_type = 1
        for file_type_code, data_io in kwargs.get('data_io_map').items():
            if type(kwargs.get('data_io')) is data_io:
                file_type = file_type_code

        statistics_conf = {
            'path': self.source_path,
            'method': compare_evaluation_code,
            'file_type': file_type,
        }

        statistics = StatisticsEngine(**statistics_conf).submit(self.job_id, df_output=False)
        return {
            'source_statistics': statistics
        }
