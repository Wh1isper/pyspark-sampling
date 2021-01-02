"""
处理模型基类
"""
from typing import Any, Dict
import logging
from sparksampling.utilities import CustomErrorWithCode


class BaseProcessModule(object):
    logger = logging.getLogger('YAB_APPLICATION')
    required_keys = {}

    def __init__(self):
        self._request_data = None
        self._kw = None

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

    def error_response(self, e: CustomErrorWithCode):
        return {
            'code': e.code,
            'msg': e.errorinfo,
            'data': {},
        }


class EmptyProcessModule(BaseProcessModule):
    async def process(self) -> Dict[str, Any]:
        response_data = {
            'code': 0,
            'msg': '',
            'data': {},
        }
        try:
            # do something
            ...
        except CustomErrorWithCode as e:
            response_data = self.error_response(e)
        return response_data
