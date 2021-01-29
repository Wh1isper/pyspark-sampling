from sparksampling.core.engine import StatisticsEngine
from sparksampling.core.engine.base_engine import SparkJobEngine
from sparksampling.utilities.var import EVALUATION_DNN_METHOD, EVALUATION_TESTING_METHOD, EVALUATION_COMPARE_METHOD
from sparksampling.core.job.evaluationjob import DNNStatisticsJob, HypothesisTestStatisticsJob
from sparksampling.core.job.evaluationjob.compare_statistics import CompareStatisticsJob

from sparksampling.customize.custom_config import extra_evaluation_job, compare_evaluation_code


class EvaluationEngine(SparkJobEngine):
    job_map = {
        EVALUATION_COMPARE_METHOD: CompareStatisticsJob,
        EVALUATION_DNN_METHOD: DNNStatisticsJob,
        EVALUATION_TESTING_METHOD: HypothesisTestStatisticsJob
    }
    job_map.update(extra_evaluation_job)

    def prepare(self, *args, **kwargs) -> dict:
        if type(self.job) is not CompareStatisticsJob:
            return {}
        file_type = 0
        for file_type_code, data_io in self.data_io_map.items():
            if type(self.data_io) is data_io:
                file_type = file_type_code

        statistics_conf = {
            'path': self.job.source_path,
            'method': compare_evaluation_code,
            'file_type': file_type,
        }

        statistics = StatisticsEngine(**statistics_conf).submit(self.job_id, df_output=False)
        return {
            'source_statistics': statistics
        }
