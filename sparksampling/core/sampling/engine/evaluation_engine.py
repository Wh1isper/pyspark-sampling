from sparksampling.core.sampling.engine.base_engine import SparkJobEngine
from sparksampling.utilities.var import EVALUATION_DNN_METHOD, EVALUATION_TESTING_METHOD
from sparksampling.core.sampling.job.evaluationjob import DNNStatisticsJob, HypothesisTestStatisticsJob

from sparksampling.customize.custom_config import extra_evaluation_job


class EvaluationEngine(SparkJobEngine):
    job_map = {
        EVALUATION_DNN_METHOD: DNNStatisticsJob,
        EVALUATION_TESTING_METHOD: HypothesisTestStatisticsJob
    }
    job_map.update(extra_evaluation_job)
