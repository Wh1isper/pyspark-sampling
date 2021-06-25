from sparksampling.core.dataio.base_dataio import BaseDataIO
from pyspark.sql import DataFrame


class ConfigurableDataIO(BaseDataIO):
    type_map = {
        'header': bool,
        'sep': str,
        'inferSchema': bool,
        'write_path': str,
        'format': str,
    }

    def __init__(self, spark, path, *args, **kwargs):
        super(ConfigurableDataIO, self).__init__(spark, path, separate=True, *args, **kwargs)
        self.header = kwargs.get('header', True)
        self.format = kwargs.get('format', 'csv')
        self.sep = kwargs.get('sep', '\001')
        self.inferSchema = kwargs.get('inferSchema')
        self.original_num_partitions = None
        self.write_path = kwargs.get('write_path', self.write_path)

    def _read(self, path=None, *args, **kwargs):
        path = path or self.path
        reader = self.spark.read.format(self.format)
        reader = self.set_options(reader)
        df = reader.load(path)
        if not self.original_num_partitions:
            self.original_num_partitions = df.rdd.getNumPartitions()
        df.show()
        return df

    def _write(self, df: DataFrame):
        if self.original_num_partitions:
            df.repartition(self.original_num_partitions)
        writer = df.write.format(self.format)
        writer = self.set_options(writer)
        writer.save(self.write_path, mode='overwrite')
        if df.is_cached:
            df.unpersist()
        return self.write_path

    def set_options(self, optional_obj):
        for k in self.type_map.keys():
            c = getattr(self, k)
            if c and k != 'format':
                optional_obj = optional_obj.option(k, c)
        return optional_obj
