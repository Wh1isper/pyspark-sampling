"""
路由表
"""

from sparksampling.handler import BaseProcessHandler, SingletonHandler
from sparksampling.handler.processmodule import DummyProcessModule, SamplingProcessModule, MLSamplingProcessModule, \
    QueryJobProcessModule, StatisticsProcessModule, QueryListProcessModule, EvaluationProcessModule, \
    QueryEvaluationListProcessModule, QueryEvaluationJobProcessModule


class HelloHandler(SingletonHandler):
    async def get(self):
        self.logger.info('Debug Mod is Running...')
        self.write({
            'code': 0,
            'msg': 'Hello World!',
            'data': {}
        })


sampling_handlers = [
    (r'/v1/sampling/simplejob/(.*)', BaseProcessHandler, dict(processmodule=SamplingProcessModule)),
    (r'/v1/sampling/mljob/(.*)', BaseProcessHandler, dict(processmodule=MLSamplingProcessModule)),
]
query_handlers = [
    (r'/v1/query/sampling/job/(.*)', SingletonHandler, dict(processmodule=QueryJobProcessModule)),
    (r'/v1/query/sampling/list/(.*)', SingletonHandler, dict(processmodule=QueryListProcessModule)),
    (r'/v1/query/evaluation/job/(.*)', SingletonHandler, dict(processmodule=QueryEvaluationJobProcessModule)),
    (r'/v1/query/evaluation/list/(.*)', SingletonHandler, dict(processmodule=QueryEvaluationListProcessModule)),

]

evaluation_handlers = [
    (r'/v1/evaluation/statistics/(.*)', SingletonHandler, dict(processmodule=StatisticsProcessModule)),
    (r'/v1/evaluation/job/(.*)', BaseProcessHandler, dict(processmodule=EvaluationProcessModule)),
]

test_handlers = [
    (r'/', HelloHandler, dict(processmodule=DummyProcessModule)),
]

debug_handlers = sampling_handlers + query_handlers + evaluation_handlers + test_handlers
