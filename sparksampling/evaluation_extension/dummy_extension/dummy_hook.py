from sparksampling.engine.dummy_engine import DummyEngine
from sparksampling.evaluation.base_hook import BaseEvaluationHook


class DummyExtensionHook(BaseEvaluationHook):
    pass


DummyExtensionHook.register_pre_hook(DummyEngine)
