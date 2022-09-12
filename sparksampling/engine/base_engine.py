from sparksampling.mixin import WorkerManagerMixin, SparkMixin


class BaseEngine(WorkerManagerMixin):
    guarantee_worker = 10
    evaluation_hook = set()

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

    @classmethod
    def register(cls, hook):
        if hook in cls.evaluation_hook:
            return
        cls.log.info(f'Adding evaluation hook: {hook.__name__} to {cls.__name__}')
        cls.evaluation_hook.add(hook)


class SparkBaseEngine(BaseEngine, SparkMixin):

    @classmethod
    def stop(cls, parent, job_id=None):
        # SparkStopEngine will cancel spark job
        return
