"""
file path and config <-> dataFrame
"""
from pyspark.sql import SparkSession, DataFrame


class BaseDataIO(object):
    def __init__(self, spark: SparkSession, path):
        self.spark = spark
        self.path = path

    def _convert_path(self):
        # todo: convert path
        return self.path+'.sampled'

class CsvDataIO(BaseDataIO):
    def __init__(self, spark, path):
        super(CsvDataIO, self).__init__(spark, path)

    def read(self):
        df = self.spark.read.csv(self.path).cache()
        return df

    def write(self, df: DataFrame):
        df.write.csv(self._convert_path())
