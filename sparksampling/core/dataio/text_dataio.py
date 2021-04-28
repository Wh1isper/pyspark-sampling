from sparksampling.core.dataio.base_dataio import BaseDataIO
from pyspark.sql import DataFrame


class TextDataIO(BaseDataIO):
    def __init__(self, spark, path, *args, **kwargs):
        super(TextDataIO, self).__init__(spark, path, args, kwargs)

    def _read(self, path=None, *args, **kwargs):
        path = path or self.path
        df = self.spark.read.text(path)
        return df

    def _write(self, df: DataFrame):
        df.write.text(self.write_path)
        return self.write_path
