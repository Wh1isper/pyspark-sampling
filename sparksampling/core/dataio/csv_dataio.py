from sparksampling.core.dataio.base_dataio import BaseDataIO
from pyspark.sql import DataFrame


class CsvDataIO(BaseDataIO):
    def __init__(self, spark, path, *args, **kwargs):
        super(CsvDataIO, self).__init__(spark, path, args, kwargs)
        self.header = kwargs.get('with_header', True)
        self.original_num_partitions = None

    def _read(self, path=None, *args, **kwargs):
        path = path or self.path
        df = self.spark.read.csv(path, header=self.header).cache()
        if not self.original_num_partitions:
            self.original_num_partitions = df.rdd.getNumPartitions()
        return df

    def _write(self, df: DataFrame):
        if self.original_num_partitions:
            df.repartition(self.original_num_partitions)
        df.write.csv(self.write_path, mode='overwrite', header=self.header)
        return self.write_path

    def copy(self):
        return
