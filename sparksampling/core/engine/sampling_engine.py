"""
data_io -> dataFrame -> sampling_job -> data_io
"""
from sparksampling.core.job import SimpleRandomSamplingJob, StratifiedSamplingJob, SmoteSamplingJob, ImbENNSamplingJob, \
    SparkENNSamplingJob, ImbSmoteSamplingJob, SmoteENNSamplingJob, ImbSMOTEENNSamplingJob
from sparksampling.utilities import from_path_import
from sparksampling.var import SIMPLE_RANDOM_SAMPLING_METHOD, STRATIFIED_SAMPLING_METHOD, SPARK_SMOTE_SAMPLING_METHOD, \
    IMB_SMOTE_SAMPLING_METHOD, IMB_ENN_SAMPLING_METHOD, SPARK_ENN_SAMPLING_METHOD, SPARK_SMOTE_ENN_SAMPLING_METHOD, \
    IMB_SMOTE_ENN_SAMPLING_METHOD
from sparksampling.core.engine.base_engine import SparkJobEngine
from sparksampling.config import CUSTOM_CONFIG_FILE

extra_sampling_job = from_path_import("extra_sampling_job", CUSTOM_CONFIG_FILE, "extra_sampling_job")


class SamplingEngine(SparkJobEngine):
    job_map = {
        SIMPLE_RANDOM_SAMPLING_METHOD: SimpleRandomSamplingJob,
        STRATIFIED_SAMPLING_METHOD: StratifiedSamplingJob,
        SPARK_SMOTE_SAMPLING_METHOD: SmoteSamplingJob,
        SPARK_ENN_SAMPLING_METHOD: SparkENNSamplingJob,
        SPARK_SMOTE_ENN_SAMPLING_METHOD: SmoteENNSamplingJob,
        IMB_SMOTE_SAMPLING_METHOD: ImbSmoteSamplingJob,
        IMB_ENN_SAMPLING_METHOD: ImbENNSamplingJob,
        IMB_SMOTE_ENN_SAMPLING_METHOD: ImbSMOTEENNSamplingJob,
    }

    job_map.update(extra_sampling_job)
