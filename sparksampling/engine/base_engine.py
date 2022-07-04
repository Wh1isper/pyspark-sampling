from sparksampling.mixin import WorkerManagerMixin, SparkMixin


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


class SparkBaseEngine(BaseEngine, SparkMixin):
    @classmethod
    def stop(cls, parent, job_id=None):
        # SparkStopEngine will cancel spark job
        return
