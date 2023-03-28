import functools
import logging
import threading
from functools import wraps

from pyspark.sql import SparkSession

from sparksampling.config import SPARK_CONF
from sparksampling.error import ExhaustedError


def with_lock(func):
    # 装饰器，封装需要锁的函数
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        self._acquire_lock()
        try:
            result = func(self, *args, **kwargs)
        finally:
            self._release_lock()
        return result

    return wrapper


class LogMixin(object):
    logger = logging.getLogger("sparksampling")
    log = logger


class LockMixin(object):
    # Mixin for a class need a lock
    _lock = threading.RLock()

    @classmethod
    def _acquire_lock(cls):
        """
        Acquire the class-level lock for serializing access to shared data.

        This should be released with _release_lock().
        """
        if cls._lock:
            cls._lock.acquire()

    @classmethod
    def _release_lock(cls):
        """
        Release the class-level lock acquired by calling _acquire_lock().
        """
        if cls._lock:
            cls._lock.release()


def acquire_worker(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        self._allocate_worker()
        try:
            r = func(self, *args, **kwargs)
        finally:
            self._release_worker()
        return r

    return wrapper


class WorkerManagerMixin(LockMixin, LogMixin):
    # Need to set guarantee_worker before use it
    guarantee_worker = 0

    @classmethod
    @with_lock
    def _allocate_worker(cls):
        if not cls.guarantee_worker:
            raise ExhaustedError(f"No enough worker for {cls.__name__}...")
        cls.guarantee_worker = cls.guarantee_worker - 1
        cls.logger.debug(f"{cls.__name__} acquire worker:{cls.guarantee_worker} left")

    @classmethod
    @with_lock
    def _release_worker(cls):
        cls.guarantee_worker = cls.guarantee_worker + 1
        cls.logger.debug(f"{cls.__name__} release worker, have {cls.guarantee_worker} now")


def record_job_id(method):
    """
    Decorate methods
    Set job_id to Engine, Currently supports spark
    """

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        job_id = kwargs.get("job_id", getattr(self, "job_id", None))
        if isinstance(self, SparkMixin) and job_id:
            self.log.info(f"Setting Spark job group: {job_id}")
            self.spark.sparkContext.setLogLevel("ERROR")
            self.spark.sparkContext.setJobGroup(
                job_id, f"Submitted by {self.__class__.__name__}", interruptOnCancel=True
            )
            # config for scheduler.mode: FAIR
            self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", job_id)
        result = method(self, *args, **kwargs)
        return result

    return wrapper


class SparkMixin(object):
    _spark = None

    @property
    def spark(self) -> SparkSession:
        if self._spark:
            return self._spark

        if not hasattr(self, "parent"):
            return SparkSession.builder.config(conf=SPARK_CONF).getOrCreate()
        spark = getattr(self.parent, "spark")
        if not spark:
            conf = getattr(self.parent, "spark_config", SPARK_CONF)
            spark = SparkSession.builder.config(conf=conf).getOrCreate()

        self._spark = spark
        return spark
