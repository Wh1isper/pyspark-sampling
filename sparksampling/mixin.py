import logging
import threading
from functools import wraps

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
