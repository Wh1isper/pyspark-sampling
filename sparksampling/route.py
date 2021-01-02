from sparksampling.manager.process_handler import BaseProcessHandler
from sparksampling.manager.processmodule.base_process_module import EmptyProcessModule


class HelloHandler(BaseProcessHandler):
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
    (r'/', HelloHandler, dict(processmodule=EmptyProcessModule)),
]
