from sparksampling.engine.dummy_engine import DummyEngine
from sparksampling.evaluation.base_hook import BaseEvaluationHook


class DummyHook(BaseEvaluationHook):
    pass


DummyEngine.register_pre_hook(DummyHook)
