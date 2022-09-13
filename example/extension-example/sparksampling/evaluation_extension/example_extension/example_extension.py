from sparksampling.engine.dummy_engine import DummyEngine
from sparksampling.evaluation.base_hook import BaseEvaluationHook


class ExampleExtensionHook(BaseEvaluationHook):
    pass


ExampleExtensionHook.register_pre_hook(DummyEngine)
