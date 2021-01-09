from pyspark.sql import SparkSession, DataFrame
import time
from sparksampling.core import Logger

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
        raise NotImplementedError

    def write(self, *args, **kwargs):
        self.logger.info(f'Write to {self.write_path}')
        return self._write(*args, **kwargs)

    def _write(self, *args, **kwargs):
        raise NotImplementedError

    def get_sampled_data_path(self):
        return self.write_path
