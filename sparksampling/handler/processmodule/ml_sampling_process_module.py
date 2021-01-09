from sparksampling.handler.processmodule.sampling_process_module import SamplingProcessModule
from sparksampling.core.sampling.engine import MLSamplingEngine
from typing import Dict


class MLSamplingProcessModule(SamplingProcessModule):
    # todo impl
    required_keys = {
        ...
    }

    def format_conf(self, request_data: Dict):
        ...

    def config_engine(self, conf) -> MLSamplingEngine:
        ...
