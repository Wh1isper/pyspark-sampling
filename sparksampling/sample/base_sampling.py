from pyspark.sql import DataFrame

from sparksampling.mixin import LogMixin, SparkMixin


class SparkBaseSamplingJob(SparkMixin, LogMixin):
    cls_args = []

    def __init__(self, *args, **kwargs):
        ...

    @classmethod
    def get_init_conf(cls, conf):
        if not cls.cls_args:
            cls.log.warning("No parameters to initialize ?")
            return dict
        init_conf = dict()
        for arg in cls.cls_args:
            if conf.get(arg):
                init_conf[arg] = conf.get(arg)
        return init_conf

    def run(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError
