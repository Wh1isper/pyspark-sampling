from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from sparksampling.config import SPARK_CONF
from sparksampling.core import Logger
from sparksampling.core.dataio import CsvDataIO, TextDataIO
from sparksampling.utilities.var import FILE_TYPE_TEXT, FILE_TYPE_CSV


class BaseEngine(Logger):
    data_io_map = {
        FILE_TYPE_TEXT: TextDataIO,
        FILE_TYPE_CSV: CsvDataIO
    }
    job_map = {}
    conf = SPARK_CONF
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    def submit(self, job_id):
        raise NotImplementedError

    def check_map(self, file_type, method):
        return self.data_io_map.get(file_type) and self.job_map.get(method)
