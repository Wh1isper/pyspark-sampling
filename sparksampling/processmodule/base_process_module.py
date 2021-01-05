"""
处理模型基类
"""
from typing import Any, Dict
import logging
from sparksampling.utilities import CustomErrorWithCode, DatabaseConnector


class BaseProcessModule(object):
    logger = logging.getLogger('SAMPLING')
    required_keys = {}

    def __init__(self):
        super(BaseProcessModule, self).__init__()
        self._request_data = None
        self._kw = None

    async def prepare(self, data, kw, *args, **kwargs):
        """
        前置工作，配置sql engine，格式化数据以及prepare_hook
        子类应使用prepare_hook而非覆写这个方法

        Returns:

        """
        self.engine = await DatabaseConnector.engine
        self.param_format(data, kw)
        await self.prepare_hook(args, kwargs)

    async def prepare_hook(self, *args, **kwargs):
        """
        在默认的前置工作执行完之后执行，子类自定义的处理程序应该在这里

        Returns:

        """
        ...

    def param_format(self, data=None, kw=None):
        """
        在process()函数之前被调用
        Args:
            data: request data
            kw: keyword

        Returns:

        """
        self._request_data = data
        self._kw = kw

    async def process(self) -> Dict[str, Any]:
        """
        覆写这个函数，完成所需功能
        Returns:
            response_data:Dict，handler会将其转换成json
        """
        raise NotImplementedError(f"No Processing...Check Implementation: {self.__class__.__name__}")

    def check_param(self, request_data):
        missing_keys = set(self.required_keys) - set(request_data.keys())
        if missing_keys:
            raise TypeError(f"Missing Param: {missing_keys}")


class DummyProcessModule(BaseProcessModule):
    def __init__(self):
        super(DummyProcessModule, self).__init__()

    async def process(self) -> Dict[str, Any]:
        response_data = {
            'code': 0,
            'msg': '',
            'data': {},
        }
        try:
            async with self.engine.acquire() as conn:
                ret = await conn.execute("SELECT * FROM test_table")
                print(await ret.fetchone())
        except CustomErrorWithCode as e:
            response_data = e.error_response()
        return response_data
