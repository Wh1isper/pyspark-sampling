from sparksampling.handler import BaseProcessHandler
from sparksampling.handler.processmodule import BaseProcessModule


class SingletonHandler(BaseProcessHandler):
    """
    单例ProcessModule的handler，为简单任务设计
    """
    processmodule = None

    def initialize(self, processmodule: type = BaseProcessModule):
        if not self.processmodule:
            self.processmodule = processmodule()
