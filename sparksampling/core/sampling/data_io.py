"""
file path and config <-> dataFrame
"""
from pyspark.sql import SparkSession, DataFrame
import time
from sparksampling.core.sampling.base import Logger


class BaseDataIO(Logger):

    def __init__(self, spark: SparkSession, path, *args, **kwargs):
        self.logger.info(f"Init DataIo for {path} with args:{args}, kwargs:{kwargs}")
        self.spark = spark
        self.path = path
        self.write_path = self.__convert_path()

    def __convert_path(self):
        return f"{self.path}-sampled-{time.time()}"

    def read(self, *args, **kwargs) -> DataFrame:
        self.logger.info(f"Read from {self.path}")
        return self._read(*args, **kwargs)

    def _read(self, header=True, *args, **kwargs):
        raise not NotImplementedError

    def write(self, *args, **kwargs):
        self.logger.info(f'Write to {self.write_path}')
        return self._write(*args, **kwargs)

    def _write(self, *args, **kwargs):
        raise NotImplementedError

    def get_sampled_data_path(self):
        return self.write_path


class CsvDataIO(BaseDataIO):
    def __init__(self, spark, path, *args, **kwargs):
        super(CsvDataIO, self).__init__(spark, path, args, kwargs)
        self.header = kwargs.get('header', True)

    def _read(self, *args, **kwargs):
        df = self.spark.read.csv(self.path, header=self.header).cache()
        return df

    def _write(self, df: DataFrame):
        df.write.csv(self.write_path, mode='overwrite')
        return self.write_path


class TextDataIO(BaseDataIO):
    def __init__(self, spark, path, *args, **kwargs):
        super(TextDataIO, self).__init__(spark, path, args, kwargs)

    def _read(self, *args, **kwargs):
        df = self.spark.read.text(self.path).cache()
        return df

    def _write(self, df: DataFrame):
        df.write.text(self.write_path)
        return self.write_path
