from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from sparksampling.core import Logger


class BaseEngine(Logger):
    data_io_map = {}
    sampling_job_map = {}
    conf = SparkConf()
    spark = SparkSession.builder.config(conf=conf).master('local').appName('Spark Sampling').getOrCreate()

    def submit(self):
        raise NotImplementedError
