import functools

from pyspark.sql import SparkSession

from sparksampling.config import SPARK_CONF
from sparksampling.mixin import WorkerManagerMixin, LogMixin


def record_job_id(method):
    """
    Decorate methods
    Set job_id to Engine, Currently supports spark
    """

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        job_id = kwargs.get('job_id', getattr(self, 'job_id', None))
        if isinstance(self, SparkMixin) and job_id:
            self.log.info(f'Setting Spark job group: {job_id}')
            self.spark.sparkContext.setLogLevel('ERROR')
            self.spark.sparkContext.setJobGroup(job_id, f'Submitted by {self.__class__.__name__}',
                                                interruptOnCancel=True)
            # config for scheduler.mode: FAIR
            self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", job_id)
        result = method(self, *args, **kwargs)
        return result

    return wrapper


class BaseEngine(WorkerManagerMixin):
    guarantee_worker = 10

    def submit(self, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def stop(cls, parent, job_id=None):
        raise NotImplementedError

    @classmethod
    def config(cls, kwargs):
        raise NotImplementedError

    @classmethod
    def is_matching(cls, request_type):
        return False


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


class SparkBaseEngine(BaseEngine, SparkMixin):
    @classmethod
    def stop(cls, parent, job_id=None):
        # SparkStopEngine will cancel spark job
        return
