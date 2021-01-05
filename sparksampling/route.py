"""
路由表
"""

from sparksampling.handler import BaseProcessHandler, SingletonHandler
from sparksampling.processmodule import DummyProcessModule


class HelloHandler(SingletonHandler):
    async def get(self):
        self.logger.info('Debug Mod is Running...')
        self.write({
            'code': 0,
            'msg': 'Hello World!',
            'data': {}
        })


default_handlers = [

]

test_handlers = [
    (r'/', HelloHandler, dict(processmodule=DummyProcessModule)),
]
