from sparksampling.core.engine import StatisticsEngine
from sparksampling.core.engine.base_engine import SparkJobEngine
from sparksampling.utilities.var import EVALUATION_DNN_METHOD, EVALUATION_TESTING_METHOD, EVALUATION_COMPARE_METHOD
from sparksampling.core.job.evaluationjob import DNNEvaluationJob, HypothesisTestEvaluationJob
from sparksampling.core.job.evaluationjob.compare_evaluation import CompareEvaluationJob

from sparksampling.customize.custom_config import extra_evaluation_job, compare_evaluation_code


class EvaluationEngine(SparkJobEngine):
    job_map = {
        EVALUATION_COMPARE_METHOD: CompareEvaluationJob,
        EVALUATION_DNN_METHOD: DNNEvaluationJob,
        EVALUATION_TESTING_METHOD: HypothesisTestEvaluationJob
    }
    job_map.update(extra_evaluation_job)

    def prepare(self, *args, **kwargs) -> dict:
        return self.job.prepare(**{
            'data_io_map': self.data_io_map,
            'data_io': self.data_io
        })
