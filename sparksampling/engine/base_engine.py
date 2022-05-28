import functools
from typing import Dict

from pyspark.sql import SparkSession

from sparksampling.config import SPARK_CONF
from sparksampling.mixin import LogMixin


def record_job_id(method):
    """Decorate methods
    根据不同引擎设置任务id，目前只支持SparkJobEngine
    """

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        job_id = kwargs.get('job_id', getattr(self, 'job_id', None))
        if isinstance(self, SparkMixin) and job_id:
            self.log.info(f'Setting sampling job id: {job_id}')
            self.spark.sparkContext.setLogLevel('ERROR')
            self.spark.sparkContext.setJobGroup(job_id, 'sampling job', interruptOnCancel=True)
            # config for scheduler.mode: FAIR
            self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", job_id)
        result = method(self, *args, **kwargs)
        return result

    return wrapper


class BaseEngine(LogMixin):
    def submit(self, *args, **kwargs):
        raise NotImplementedError

    def stop(self, job_id=None):
        raise NotImplementedError


class SparkMixin(LogMixin):
    @property
    def spark(self) -> SparkSession:
        if not hasattr(self, 'parent'):
            return SparkSession.builder.config(conf=SPARK_CONF).getOrCreate()
        spark = getattr(self.parent, 'spark')
        if not spark:
            conf = getattr(self.parent, 'spark_config', SPARK_CONF)
            spark = SparkSession.builder.config(conf=conf).getOrCreate()
        return spark
