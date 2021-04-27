from sparksampling.handler.processmodule.sampling_process_module import SamplingProcessModule
import random


class MLSamplingProcessModule(SamplingProcessModule):
    def job_conf(self, conf):
        job_conf = self.__default_conf(conf)
        job_conf.update(self.__customize_conf(conf))
        return job_conf

    def __default_conf(self, conf):
        return {
            'col_key': conf.get('key')
        }

    def __customize_conf(self, conf):
        return {}
