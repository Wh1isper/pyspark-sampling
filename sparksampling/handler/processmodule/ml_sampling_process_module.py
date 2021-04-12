from sparksampling.handler.processmodule.sampling_process_module import SamplingProcessModule
import random


class MLSamplingProcessModule(SamplingProcessModule):
    def job_conf(self, conf):
        job_conf = self.__smote_conf(conf)
        job_conf.update(self.__customize_conf(conf))
        return job_conf

    def __smote_conf(self, conf):
        return {
            'k': conf.get('k', 3),
            'bucket_length': conf.get('bucket_length', 10),
            'multiplier': conf.get('multiplier', 2),
            'seed': conf.get('seed', random.randint(1, 65535)),
            'restore': conf.get('restore', True),
            'col_key': conf.get('key')
        }

    def __customize_conf(self, conf):
        return {}
