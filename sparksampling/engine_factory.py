import importlib
from pprint import pformat

from google.protobuf.json_format import MessageToDict

from sparksampling.error import BadParamError, ProcessError
from sparksampling.mixin import LogMixin


class EngineFactory(LogMixin):
    engine_cls = set()

    @classmethod
    def choose_engine(cls, request_type):
        engine_cls = None
        for engine in cls.engine_cls:
            if engine.is_matching(request_type):
                if engine_cls:
                    raise ProcessError(
                        f"Duplicated Engine Found: {engine.__name__} and {engine_cls.__name__}"
                    )
                engine_cls = engine
        return engine_cls

    @classmethod
    def get_engine(cls, parent, request_type, **kwargs):
        engine_cls = cls.choose_engine(request_type)
        if not engine_cls:
            raise BadParamError(f"No matching engine for this job, \n{pformat(kwargs, indent=2)}")
        cls.log.info(f"Using {engine_cls.__name__}")
        return engine_cls(parent, **engine_cls.config(kwargs))

    @classmethod
    def message_to_dict(cls, message):
        data = MessageToDict(message, use_integers_for_enums=True, preserving_proto_field_name=True)
        return data

    @classmethod
    def cancel_job(cls, parent, job_id):
        for engine in cls.engine_cls:
            engine.stop(parent, job_id)

    @classmethod
    def register(cls, engine_class):
        if engine_class in cls.engine_cls:
            return
        cls.log.info(
            f"Register engine: {engine_class.__name__}, allocate {engine_class.guarantee_worker} workers"
        )
        cls.engine_cls.add(engine_class)

    @staticmethod
    def register_all_engine():
        importlib.import_module("sparksampling.engine")

    @classmethod
    def get_engine_total_worker(cls):
        return sum(engine.guarantee_worker for engine in cls.engine_cls)
