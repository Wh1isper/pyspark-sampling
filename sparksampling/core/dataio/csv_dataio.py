from sparksampling.core.dataio.base_dataio import BaseDataIO
from pyspark.sql import DataFrame


class CsvDataIO(BaseDataIO):
    def __init__(self, spark, path, *args, **kwargs):
        super(CsvDataIO, self).__init__(spark, path, args, kwargs)
        self.header = kwargs.get('with_header', True)
        self.__df: DataFrame = None

    def _read(self, *args, **kwargs):
        df = self.spark.read.csv(self.path, header=self.header).cache()
        self.__df = df
        return df

    def _write(self, df: DataFrame):
        if self.__df:
            self.__df.unpersist()
        df.repartition(self.__df.rdd.getNumPartitions())
        df.write.csv(self.write_path, mode='overwrite', header=self.header)
        return self.write_path

    def __del__(self):
        if self.__df:
            self.__df.unpersist()

    def copy(self):
        return
