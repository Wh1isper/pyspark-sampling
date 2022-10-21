from pyspark.sql import DataFrame

from sparksampling.engine_factory import EngineFactory
from sparksampling.evaluation.hook_msg import HookMsg


class BaseEvaluationHook():
    def process(self, df: DataFrame) -> (DataFrame, HookMsg):
        """
        :param df:
        :return:
            a processed dataframe
            a dict shows hooks metadata
        """
        raise NotImplementedError

    @classmethod
    def register_pre_hook(cls, engine):
        engine.register_pre_hook(cls)

    @classmethod
    def register_post_hook(cls, engine):
        engine.register_post_hook(cls)

    @classmethod
    def _get_all_engine(cls):
        return EngineFactory.engine_cls

    @classmethod
    def register_pre_hook_all(cls):
        for engine in cls._get_all_engine():
            cls.register_pre_hook(engine)

    @classmethod
    def register_post_hook_all(cls):
        for engine in cls._get_all_engine():
            cls.register_post_hook(engine)


class ByPassEvaluationHook(BaseEvaluationHook):
    def process(self, df):
        return df, HookMsg(self, {
            'msg': 'bypass'
        })


ByPassEvaluationHook.register_pre_hook_all()
ByPassEvaluationHook.register_post_hook_all()
