"""
处理模型基类
"""
from typing import Any, Dict
import logging

from pymysql import OperationalError

from sparksampling.config import PARALLEL
from sparksampling.core import DatabaseConnector
from sparksampling.utilities import CustomErrorWithCode, JsonDecodeError
from sparksampling.utilities.custom_error import SQLError, DBConnectError
from sparksampling.var import JOB_CREATED, JOB_CREATING

from concurrent.futures.thread import ThreadPoolExecutor


class BaseProcessModule(object):
    executor = ThreadPoolExecutor(max_workers=PARALLEL if PARALLEL else None)
    logger = logging.getLogger('SAMPLING')
    required_keys = set()

    def __init__(self):
        super(BaseProcessModule, self).__init__()
        self._request_data = None
        self._kw = None
        self.sqlengine = None
        self.job_stats = JOB_CREATING

    async def prepare(self, data, kw, *args, **kwargs):
        """
        前置工作，配置sql engine，格式化数据以及prepare_hook
        子类应使用prepare_hook而非覆写这个方法

        Returns:

        """
        try:
            self.sqlengine = await DatabaseConnector.engine
        except OperationalError as e:
            raise DBConnectError(e.args[1])
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
        覆写这个函数，完成响应
        Returns:
            response_data:Dict，handler会将其转换成json
        """
        raise NotImplementedError(f"No Processing...Check Implementation: {self.__class__.__name__}")

    async def run_job(self):
        """
        process 结束之后将返回响应，此函数在process后执行，如需要则覆写
        Returns:

        """
        ...

    def _check_json_request(self, request_data):
        if type(request_data) is not dict:
            raise JsonDecodeError

    def check_param(self, request_data):
        self.logger.info(f"Checking Param:{request_data}")
        self._check_json_request(request_data)
        missing_keys = set(self.required_keys) - set(request_data.keys())
        if missing_keys:
            raise TypeError(f"Missing Param: {missing_keys}")

    @property
    def is_job_created(self):
        return self.job_stats == JOB_CREATED


class BaseQueryProcessModule(BaseProcessModule):
    sql_table = None

    def __init__(self):
        super(BaseQueryProcessModule, self).__init__()

    def get_query_param_from_request_data(self, request_data):
        raise NotImplementedError

    @staticmethod
    async def query_job_id(conn, job_id, table):
        result = await conn.execute(table.select().where(table.c.job_id == job_id))
        details = await result.fetchone()
        return details

    async def query(self, query_param) -> dict or None:
        raise NotImplementedError

    def format_response(self, response_data, details) -> dict:
        raise NotImplementedError

    async def process(self) -> Dict[str, Any]:
        response_data = {
            'code': 0,
            'msg': "",
            'data': {}
        }
        request_data: Dict = self._request_data

        if type(request_data) is not dict:
            raise JsonDecodeError
        self.check_param(request_data)
        query_param = self.get_query_param_from_request_data(request_data)
        try:
            details = await self.query(query_param)
        except Exception as e:
            raise SQLError(str(e))
        response_data = self.format_response(response_data, details)
        return response_data


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
            async with self.sqlengine.acquire() as conn:
                ret = await conn.execute("SELECT * FROM test_table")
                print(await ret.fetchone())
        except CustomErrorWithCode as e:
            # catch or rise
            response_data = e.error_response()
        return response_data
