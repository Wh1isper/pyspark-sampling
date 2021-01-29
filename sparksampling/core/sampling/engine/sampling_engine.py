"""
data_io -> dataFrame -> sampling_job -> data_io
"""
from sparksampling.core.sampling.job import SimpleRandomSamplingJob, StratifiedSamplingJob, SmoteSamplingJob
from sparksampling.utilities.var import SIMPLE_RANDOM_SAMPLING_METHOD, STRATIFIED_SAMPLING_METHOD, SMOTE_SAMPLING_METHOD
from sparksampling.core.sampling.engine.base_engine import SparkJobEngine

from sparksampling.customize.custom_config import extra_sampling_job


class SamplingEngine(SparkJobEngine):
    job_map = {
        SIMPLE_RANDOM_SAMPLING_METHOD: SimpleRandomSamplingJob,
        STRATIFIED_SAMPLING_METHOD: StratifiedSamplingJob,
        SMOTE_SAMPLING_METHOD: SmoteSamplingJob
    }

    job_map.update(extra_sampling_job)
