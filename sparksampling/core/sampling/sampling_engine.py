"""
data_io -> dataFrame -> sampling_job -> data_io
"""
from sparksampling.core.sampling.data_io import CsvDataIO
from sparksampling.core.sampling.job import SimpleRandomSamplingJob
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


class SamplingEngine(object):
    def __init__(self):
        self.conf = SparkConf()
        self.spark = SparkSession.builder.config(conf=self.conf).master('local').appName('Spark Sampling').getOrCreate()

    def submit(self, path):
        data_io = CsvDataIO(self.spark, path)
        df = data_io.read()
        df.show(5)
        simple_job = SimpleRandomSamplingJob()
        sampled_df = simple_job.generate(df)
        sampled_df.show(5)
        data_io.write(sampled_df)
