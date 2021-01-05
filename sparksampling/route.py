"""
路由表
"""

from sparksampling.handler import BaseProcessHandler, SingletonHandler
from sparksampling.processmodule import DummyProcessModule, SamplingProcessModule


class HelloHandler(SingletonHandler):
    async def get(self):
        self.logger.info('Debug Mod is Running...')
        self.write({
            'code': 0,
            'msg': 'Hello World!',
            'data': {}
        })


default_handlers = [
    (r'/sampling', BaseProcessHandler, dict(processmodule=SamplingProcessModule)),
]

test_handlers = [
    (r'/', HelloHandler, dict(processmodule=DummyProcessModule)),
]
