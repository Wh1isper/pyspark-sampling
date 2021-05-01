"""
路由表
"""

from sparksampling.handler import BaseProcessHandler, SingletonHandler
from sparksampling.handler.processmodule import DummyProcessModule, SamplingProcessModule, MLSamplingProcessModule, \
    QueryJobProcessModule, StatisticsProcessModule, QueryListProcessModule, EvaluationProcessModule, \
    QueryEvaluationListProcessModule, QueryEvaluationJobProcessModule, CancelSamplingJobProcessModule, \
    CancelEvaluationJobProcessModule


class HelloHandler(SingletonHandler):
    async def get(self):
        self.logger.info('Debug Mod is Running...')
        self.write(await self.fetch(self.request.body))


sampling_handlers = [
    (r'/v1/sampling/simplejob/(.*)', BaseProcessHandler, dict(processmodule=SamplingProcessModule)),
    (r'/v1/sampling/mljob/(.*)', BaseProcessHandler, dict(processmodule=MLSamplingProcessModule)),
    (r'/v1/sampling/cancel/(.*)', SingletonHandler, dict(processmodule=CancelSamplingJobProcessModule)),

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
    (r'/v1/evaluation/cancel/(.*)', SingletonHandler, dict(processmodule=CancelEvaluationJobProcessModule)),

]

test_handlers = [
    (r'/', HelloHandler, dict(processmodule=DummyProcessModule)),
]

all_handlers = sampling_handlers + query_handlers + evaluation_handlers
debug_handlers = all_handlers + test_handlers
