"""
data_io -> dataFrame -> sampling_job -> data_io
"""
from sparksampling.core.sampling.job import SimpleRandomSamplingJob, StratifiedSamplingJob, SmoteSamplingJob
from sparksampling.utilities.utilities import extract_none_in_dict, get_value_by_require_dict
from sparksampling.utilities.var import SIMPLE_RANDOM_SAMPLING_METHOD, STRATIFIED_SAMPLING_METHOD, SMOTE_SAMPLING_METHOD
from sparksampling.core.sampling.engine.base_engine import BaseEngine


class SamplingEngine(BaseEngine):
    sampling_job_map = {
        SIMPLE_RANDOM_SAMPLING_METHOD: SimpleRandomSamplingJob,
        STRATIFIED_SAMPLING_METHOD: StratifiedSamplingJob,
        SMOTE_SAMPLING_METHOD: SmoteSamplingJob
    }

    def __init__(self, path, method, file_type, *args, **kwargs):
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

    def submit(self):
        self.logger.info("Submit job to Spark...")
        df = self.data_io.read()
        sampled_df = self.sample_job.generate(df)
        return self.data_io.write(sampled_df)
