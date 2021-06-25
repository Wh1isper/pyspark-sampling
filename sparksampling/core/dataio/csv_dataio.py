from sparksampling.core.dataio.base_dataio import BaseDataIO
from pyspark.sql import DataFrame


class CsvDataIO(BaseDataIO):
    type_map = {
        'with_header': bool
    }

    def __init__(self, spark, path, *args, **kwargs):
        super(CsvDataIO, self).__init__(spark, path, args, kwargs)
        self.header = kwargs.get('with_header', True)
        self.original_num_partitions = None

    def _read(self, path=None, *args, **kwargs):
        path = path or self.path
        df = self.spark.read.csv(path, header=self.header)
        if not self.original_num_partitions:
            self.original_num_partitions = df.rdd.getNumPartitions()
        return df

    def _write(self, df: DataFrame):
        if self.original_num_partitions:
            df.repartition(self.original_num_partitions)
        df.write.csv(self.write_path, mode='overwrite', header=self.header)
        if df.is_cached:
            df.unpersist()
        return self.write_path
