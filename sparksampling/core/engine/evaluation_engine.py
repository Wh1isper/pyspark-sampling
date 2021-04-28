from sparksampling.core.engine.base_engine import SparkJobEngine
from sparksampling.utilities import from_path_import
from sparksampling.var import EVALUATION_DNN_METHOD, EVALUATION_TESTING_METHOD, EVALUATION_COMPARE_METHOD, \
    EVALUATION_KMEANS_METHOD
from sparksampling.core.job.evaluationjob import DNNEvaluationJob, HypothesisTestEvaluationJob
from sparksampling.core.job.evaluationjob.compare_evaluation import CompareEvaluationJob
from sparksampling.core.job.evaluationjob.kmeans_evaluation import KmeansEvaluationJob
from sparksampling.config import CUSTOM_CONFIG_FILE

extra_evaluation_job = from_path_import("extra_evaluation_job", CUSTOM_CONFIG_FILE, "extra_evaluation_job")


class EvaluationEngine(SparkJobEngine):
    job_map = {
        EVALUATION_COMPARE_METHOD: CompareEvaluationJob,
        EVALUATION_DNN_METHOD: DNNEvaluationJob,
        EVALUATION_TESTING_METHOD: HypothesisTestEvaluationJob,
        EVALUATION_KMEANS_METHOD: KmeansEvaluationJob
    }
    job_map.update(extra_evaluation_job)

    def prepare(self, *args, **kwargs) -> dict:
        return self.job.prepare(**{
            'data_io_map': self.data_io_map,
            'data_io': self.data_io
        })
