"""
data_io -> dataFrame -> sampling_job -> data_io
"""
from sparksampling.core.dataio import CsvDataIO, TextDataIO
from sparksampling.core.sampling.job import SimpleRandomSamplingJob, StratifiedSamplingJob
from sparksampling.utilities.var import FILE_TYPE_TEXT, FILE_TYPE_CSV, SIMPLE_RANDOM_SAMPLING_METHOD, \
    STRATIFIED_SAMPLING_METHOD
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from sparksampling.core.sampling.engine.base_engine import BaseEngine


class SimpleSamplingEngine(BaseEngine):
    data_io_map = {
        FILE_TYPE_TEXT: TextDataIO,
        FILE_TYPE_CSV: CsvDataIO
    }

    sampling_job_map = {
        SIMPLE_RANDOM_SAMPLING_METHOD: SimpleRandomSamplingJob,
        STRATIFIED_SAMPLING_METHOD: StratifiedSamplingJob,
    }

    def __init__(self, path, method, fraction, file_type, with_header, seed, col_key):
        self.data_io = self.data_io_map[file_type](spark=self.spark, path=path, header=with_header)
        self.sample_job = self.sampling_job_map[method](fraction=fraction, seed=seed, key=col_key)

    def submit(self):
        self.logger.info("Submit job to Spark...")
        df = self.data_io.read()
        sampled_df = self.sample_job.generate(df)
        return self.data_io.write(sampled_df)