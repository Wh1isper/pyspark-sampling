"""
路由表
"""

from sparksampling.handler import BaseProcessHandler, SingletonHandler
from sparksampling.handler.processmodule import DummyProcessModule, SamplingProcessModule, MLSamplingProcessModule, \
    QueryProcessModule


class HelloHandler(SingletonHandler):
    async def get(self):
        self.logger.info('Debug Mod is Running...')
        self.write({
            'code': 0,
            'msg': 'Hello World!',
            'data': {}
        })


default_handlers = [
    (r'/v1/sampling/simplejob/(.*)', BaseProcessHandler, dict(processmodule=SamplingProcessModule)),
    (r'/v1/sampling/mljob/(.*)', BaseProcessHandler, dict(processmodule=MLSamplingProcessModule)),
    (r'/v1/sampling/query/(.*)', SingletonHandler, dict(processmodule=QueryProcessModule))
]

test_handlers = [
    (r'/', HelloHandler, dict(processmodule=DummyProcessModule)),
]
