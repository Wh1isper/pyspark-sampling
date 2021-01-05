from sparksampling.processmodule import BaseProcessModule
from typing import Dict, Any
from sparksampling.utilities import JsonDecodeError
from sparksampling.core import SamplingEngine


class SamplingProcessModule(BaseProcessModule):
    # todo: required_keys
    required_keys = {

    }

    async def process(self) -> Dict[str, Any]:
        """
        配置抽样任务，写数据库，读取数据并提交抽样任务
        Returns:

        """
        response_data = {
            'code': 0,
            'msg': "",
            'data': {}
        }
        request_data = self._request_data

        if type(request_data) is not dict:
            raise JsonDecodeError
        self.check_param(response_data)

        # todo config engine
        engine = self.config_engine()
        engine.submit('hdfs://localhost:9000/dataset/titanic/train.csv')
        response_data = await self.record_in_db()

        return response_data

    def config_engine(self) -> SamplingEngine():
        return SamplingEngine()

    async def record_in_db(self) -> Dict[str, Any]:
        async with self.engine.acquire() as conn:
            ...
        return {}
