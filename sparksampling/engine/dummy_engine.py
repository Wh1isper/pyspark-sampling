from sparksampling.engine.base_engine import BaseEngine


class DummyEngine(BaseEngine):
    guarantee_worker = 0

    @classmethod
    def register_pre_hook(cls, hook):
        cls.log.debug(
            f"Adding pre evaluation hook: {hook.__name__} to {cls.__name__}, This is a test output with no effect"
        )

    @classmethod
    def register_post_hook(cls, hook):
        cls.log.debug(
            f"Adding post evaluation hook: {hook.__name__} to {cls.__name__}, This is a test output with no effect"
        )
