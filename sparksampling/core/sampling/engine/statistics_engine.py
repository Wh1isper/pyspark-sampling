from sparksampling.core.sampling.engine.base_engine import BaseEngine
from sparksampling.utilities.custom_error import BadParamError, JobTypeError, JobKeyError, JobProcessError
from sparksampling.utilities.utilities import extract_none_in_dict, get_value_by_require_dict
from sparksampling.utilities.var import STATISTICS_BASIC_METHOD, EVALUATION_DNN_METHOD, EVALUATION_TESTING_METHOD
from sparksampling.core.sampling.job.evaluationjob import BasicStatisticsJob, DNNStatisticsJob, \
    HypothesisTestStatisticsJob


class EvaluationEngine(BaseEngine):
    job_map = {
        STATISTICS_BASIC_METHOD: BasicStatisticsJob,
        EVALUATION_DNN_METHOD: DNNStatisticsJob,
        EVALUATION_TESTING_METHOD: HypothesisTestStatisticsJob
    }

    def __init__(self, path, method, file_type, *args, **kwargs):
        if not self.check_map(file_type, method):
            raise BadParamError(
                f"Sampling method or file type wrong, expected {self.data_io_map} and {self.job_map}")

        self.job_id = None
        self.path = path
        self.method = method
        self.data_io_conf = {
            'with_header': kwargs.get('with_header')
        }
        extract_none_in_dict(self.data_io_conf)

        job_class = self.job_map[method]
        self.job_conf = get_value_by_require_dict(job_class.type_map, kwargs)

        self.data_io = self.data_io_map[file_type](spark=self.spark, path=self.path, **self.data_io_conf)
        self.job = self.job_map[method](**self.job_conf)

    def submit(self, job_id=None) -> dict:
        self.job_id = job_id
        self.logger.info(f"Submit statistics job to Spark...job_id: {self.job_id}")
        try:
            df = self.data_io.read(self.job_id)
            return self.job.statistics(df, self.job_id)
        except TypeError as e:
            raise JobTypeError(str(e))
        except KeyError as e:
            raise JobKeyError(str(e))
        except Exception as e:
            raise JobProcessError(str(e))
