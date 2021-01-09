from sparksampling.core.dataio.base_dataio import BaseDataIO
from pyspark.sql import DataFrame


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