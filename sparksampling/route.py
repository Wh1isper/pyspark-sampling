"""
路由表
"""

from sparksampling.handler import BaseProcessHandler, SingletonHandler
from sparksampling.handler.processmodule import DummyProcessModule, SamplingProcessModule, MLSamplingProcessModule, \
    QueryJobProcessModule, StatisticsProcessModule, QueryListProcessModule


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
    (r'/v1/sampling/query/job/(.*)', SingletonHandler, dict(processmodule=QueryJobProcessModule)),
    (r'/v1/sampling/query/list/(.*)', SingletonHandler, dict(processmodule=QueryListProcessModule)),
]
statistics_handlers = [
    (r'/v1/statistics/(.*)', SingletonHandler, dict(processmodule=StatisticsProcessModule)),
]

test_handlers = [
    (r'/', HelloHandler, dict(processmodule=DummyProcessModule)),
]

debug_handlers = sampling_handlers + query_handlers + statistics_handlers + test_handlers
