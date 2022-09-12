from sparksampling.engine.base_engine import BaseEngine


class DummyEngine(BaseEngine):
    guarantee_worker = 0

    @classmethod
    def register(cls, hook):
        if hook in cls.evaluation_hook:
            return
        cls.log.debug(f'Adding evaluation hook: {hook.__name__} to {cls.__name__}')
        cls.evaluation_hook.add(hook)
