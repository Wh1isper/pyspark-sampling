from pyspark.sql import DataFrame, SparkSession

from sparksampling.mixin import LogMixin


class SparkBaseFileFormat(LogMixin):
    cls_args = []

    @classmethod
    def get_init_conf(cls, conf):
        if not cls.cls_args:
            cls.log.warning("No parameters to initialize?")
            return dict
        init_conf = dict()
        for arg in cls.cls_args:
            if conf.get(arg):
                init_conf[arg] = conf.get(arg)
        return init_conf

    def __init__(self, spark, *args, **kwarg):
        self.spark: SparkSession = spark

    def read(self, input_path):
        raise NotImplementedError

    def write(self, df: DataFrame, output_path, output_col=None):
        raise NotImplementedError
