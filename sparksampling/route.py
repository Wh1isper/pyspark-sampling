"""
路由表
"""

from sparksampling.handler import BaseProcessHandler, SingletonHandler
from sparksampling.handler.processmodule import DummyProcessModule, SamplingProcessModule, MLSamplingProcessModule, \
    QuerySamplingJobProcessModule, StatisticsProcessModule, QuerySamplingListProcessModule, EvaluationProcessModule, \
    QueryEvaluationListProcessModule, QueryEvaluationJobProcessModule, SamplingJobCancelProcessModule, \
    EvaluationJobCancelProcessModule, StopProcessModule


class HelloHandler(SingletonHandler):
    async def get(self):
        self.logger.info('Debug Mod is Running...')
        self.write(await self.fetch(self.request.body))


spark_handlers = [
    (r'/stop/(.*)', SingletonHandler, dict(processmodule=StopProcessModule)),
]

sampling_handlers = [
    (r'/v1/sampling/simplejob/(.*)', BaseProcessHandler, dict(processmodule=SamplingProcessModule)),
    (r'/v1/sampling/mljob/(.*)', BaseProcessHandler, dict(processmodule=MLSamplingProcessModule)),
    (r'/v1/sampling/cancel/(.*)', SingletonHandler, dict(processmodule=SamplingJobCancelProcessModule)),
]
query_handlers = [
    (r'/v1/query/sampling/job/(.*)', SingletonHandler, dict(processmodule=QuerySamplingJobProcessModule)),
    (r'/v1/query/sampling/list/(.*)', SingletonHandler, dict(processmodule=QuerySamplingListProcessModule)),
    (r'/v1/query/evaluation/job/(.*)', SingletonHandler, dict(processmodule=QueryEvaluationJobProcessModule)),
    (r'/v1/query/evaluation/list/(.*)', SingletonHandler, dict(processmodule=QueryEvaluationListProcessModule)),

]

evaluation_handlers = [
    (r'/v1/evaluation/statistics/(.*)', SingletonHandler, dict(processmodule=StatisticsProcessModule)),
    (r'/v1/evaluation/job/(.*)', BaseProcessHandler, dict(processmodule=EvaluationProcessModule)),
    (r'/v1/evaluation/cancel/(.*)', SingletonHandler, dict(processmodule=EvaluationJobCancelProcessModule)),

]

test_handlers = [
    (r'/', HelloHandler, dict(processmodule=DummyProcessModule)),
]

all_handlers = spark_handlers + sampling_handlers + query_handlers + evaluation_handlers
debug_handlers = all_handlers + test_handlers
