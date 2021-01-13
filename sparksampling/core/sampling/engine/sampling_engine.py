"""
data_io -> dataFrame -> sampling_job -> data_io
"""
from sparksampling.core.sampling.job import SimpleRandomSamplingJob, StratifiedSamplingJob, SmoteSamplingJob
from sparksampling.utilities.utilities import extract_none_in_dict, get_value_by_require_dict
from sparksampling.utilities.var import SIMPLE_RANDOM_SAMPLING_METHOD, STRATIFIED_SAMPLING_METHOD, SMOTE_SAMPLING_METHOD
from sparksampling.core.sampling.engine.base_engine import BaseEngine
from sparksampling.utilities.custom_error import JobKeyError, JobProcessError, JobTypeError, BadParamError


class SamplingEngine(BaseEngine):
    sampling_job_map = {
        SIMPLE_RANDOM_SAMPLING_METHOD: SimpleRandomSamplingJob,
        STRATIFIED_SAMPLING_METHOD: StratifiedSamplingJob,
        SMOTE_SAMPLING_METHOD: SmoteSamplingJob
    }

    def __init__(self, path, method, file_type, *args, **kwargs):
        if not self.check_map(file_type, method):
            raise BadParamError(
                f"Sampling method or file type wrong, expected {self.data_io_map} and {self.sampling_job_map}")

        self.job_id = None
        self.path = path
        self.method = method
        self.data_io_conf = {
            'with_header': kwargs.get('with_header')
        }
        extract_none_in_dict(self.data_io_conf)

        sample_job_class = self.sampling_job_map[method]
        self.sampling_job_conf = get_value_by_require_dict(sample_job_class.type_map, kwargs)

        self.data_io = self.data_io_map[file_type](spark=self.spark, path=self.path, **self.data_io_conf)
        self.sample_job = self.sampling_job_map[method](**self.sampling_job_conf)

    def submit(self, job_id):
        self.job_id = job_id
        self.logger.info(f"Submit job to Spark...job_id: {self.job_id}")
        try:
            df = self.data_io.read(self.job_id)
            sampled_df = self.sample_job.generate(df, self.job_id)
            return self.data_io.write(sampled_df)
        except TypeError as e:
            raise JobTypeError(str(e))
        except KeyError as e:
            raise JobKeyError(str(e))
        except Exception as e:
            raise JobProcessError(str(e))
