from sparksampling.core.dataio.base_dataio import BaseDataIO
from pyspark.sql import DataFrame
import pandas as pd


class DummyDataIO(BaseDataIO):
    def __init__(self, spark, path):
        super(DummyDataIO, self).__init__(spark, path)
        self.write_path = "path to write"

    def _read(self, header=True, *args, **kwargs) -> DataFrame:
        pandas_df = pd.read_csv("this is not a path")
        return self.spark.createDataFrame(pandas_df)

    def _write(self, *args, **kwargs):
        # write a file or url
        # return write path or None for failed
        return self.write_path
