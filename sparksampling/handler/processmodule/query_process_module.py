from sparksampling.handler.processmodule import BaseProcessModule
from typing import Dict, Any


class QueryProcessModule(BaseProcessModule):
    def __init__(self):
        super(QueryProcessModule, self).__init__()

    async def process(self) -> Dict[str, Any]:
        ...

