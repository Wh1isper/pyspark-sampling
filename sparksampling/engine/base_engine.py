import os
import weakref

from sparksampling.error import ProcessPostHookError, ProcessPreHookError
from sparksampling.mixin import SparkMixin, WorkerManagerMixin


class BaseEngine(WorkerManagerMixin):
    guarantee_worker = int(os.getenv("ENGINE_DEFAULT_WORKER_NUM", 10))
    evaluation_pre_hook = dict()
    evaluation_post_hook = dict()
    _cache_hook_instance = weakref.WeakValueDictionary()

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
    def register_pre_hook(cls, hook):
        evaluation_pre_hook = cls.evaluation_pre_hook.setdefault(cls, set())
        if hook in evaluation_pre_hook:
            return
        cls.log.info(f"Adding pre evaluation hook: {hook.__name__} to {cls.__name__}")
        evaluation_pre_hook.add(hook)

    @classmethod
    def register_post_hook(cls, hook):
        evaluation_post_hook = cls.evaluation_post_hook.setdefault(cls, set())

        if hook in evaluation_post_hook:
            return
        cls.log.info(f"Adding post evaluation hook: {hook.__name__} to {cls.__name__}")
        evaluation_post_hook.add(hook)

    @classmethod
    def _process_hook(cls, df, hooks, exception, period):
        metas = []
        for hook in hooks:
            try:
                df, meta = cls.get_hook_instance(hook).process(df)
                metas.append(meta.generate_proto_msg(period))
            except NotImplementedError:
                raise exception(
                    f"{period} hook raised, Not implemented hook:{hook} found in {hooks}"
                )
            except Exception as e:
                cls.log.info(
                    f"Exception when processing df: {df} with hook: {hook} in period {period}"
                )
                cls.logger.exception(e)
                raise exception(str(e))

        return df, metas

    @classmethod
    def pre_hook(cls, df):
        return cls._process_hook(
            df, cls.evaluation_pre_hook.get(cls, set()), ProcessPreHookError, "pre"
        )

    @classmethod
    def post_hook(cls, df):
        return cls._process_hook(
            df, cls.evaluation_post_hook.get(cls, set()), ProcessPostHookError, "post"
        )

    @classmethod
    def get_hook_instance(cls, hook):
        return cls._cache_hook_instance.setdefault(hook, hook())


class SparkBaseEngine(BaseEngine, SparkMixin):
    @classmethod
    def stop(cls, parent, job_id=None):
        # SparkStopEngine will cancel spark job
        return
