import re
import os
import signal
import time

import requests
from pyspark.sql import SparkSession

from sparksampling.error import CustomErrorWithCode, ExhaustedError, ProcessError
from functools import wraps
import logging

from sparksampling.proto.sampling_service_pb2 import SamplingResponse, SamplingRequest
from google.protobuf.json_format import MessageToDict

logger = logging.getLogger("sparksampling")


def hump2underline(hunp_str):
    '''
    驼峰形式字符串转成下划线形式
    :param hunp_str: 驼峰形式字符串
    :return: 字母全小写的下划线形式字符串
    '''
    # 匹配正则，匹配小写字母和大写字母的分界位置
    p = re.compile(r'([a-z]|\d)([A-Z])')
    # 这里第二个参数使用了正则分组的后向引用
    sub = re.sub(p, r'\1_\2', hunp_str).lower()
    return sub


def hump2underline_dict(data):
    # 别被迭代器骗了
    keys = list(data.keys())
    for key in keys:
        underline_key = hump2underline(key)
        if type(data[key]) is dict:
            data[underline_key] = hump2underline_dict(data[key])
        data[underline_key] = data.pop(key)
    return data


def underline2hump(underline_str):
    '''
    下划线形式字符串转成驼峰形式
    :param underline_str: 下划线形式字符串
    :return: 驼峰形式字符串
    '''
    # 这里re.sub()函数第二个替换参数用到了一个匿名回调函数，回调函数的参数x为一个匹配对象，返回值为一个处理后的字符串
    sub = re.sub(r'(_\w)', lambda x: x.group(1)[1].upper(), underline_str)
    return sub


# 异常输出
def except_execute(func):
    @wraps(func)
    def except_print(self, request, context):
        data = MessageToDict(request, preserving_proto_field_name=True)
        parent_request = SamplingRequest(**data)
        start_time = time.time()
        try:
            return func(self, request, context)
        except CustomErrorWithCode as e:
            logger.exception(e)
            return SamplingResponse(**e.error_response(),
                                    data=SamplingResponse.ResponseData(parent_request=parent_request))
        except Exception as e:
            logger.exception(e)
            return SamplingResponse(code=5000, message=str(e),
                                    data=SamplingResponse.ResponseData(parent_request=parent_request))
        finally:
            end_time = time.time()
            cls_logger = getattr(self, 'logger', logger)
            cls_logger.debug(f"execute in {str(end_time - start_time).split('.')[0]} seconds")

    return except_print


def async_except_execute(func):
    @wraps(func)
    async def except_print(self, request, context):
        data = MessageToDict(request, preserving_proto_field_name=True)
        parent_request = SamplingRequest(**data)
        start_time = time.time()
        try:
            return await func(self, request, context)
        except CustomErrorWithCode as e:
            logger.exception(e)
            return SamplingResponse(**e.error_response(),
                                    data=SamplingResponse.ResponseData(parent_request=parent_request))
        except Exception as e:
            logger.exception(e)
            return SamplingResponse(code=5000, message=str(e),
                                    data=SamplingResponse.ResponseData(parent_request=parent_request))
        finally:
            end_time = time.time()
            cls_logger = getattr(self, 'logger') or logger
            cls_logger.info(f"execute in {end_time - start_time} seconds")

    return except_print


def check_spark_session(func):
    # Check the spark driver status and exit if it is not responding
    # Server will be redeployed under K8S deployment
    def _check_spark_ui(logger, spark: SparkSession):
        try:
            if not spark.conf.get('spark.master').startswith('local'):
                return True
        except Exception as e:
            logger.exception(e)
            logger.warning(f"can't access spark.conf")
            return False

        ui_port = spark.conf.get('spark.ui.port')
        url = f"http://localhost:{ui_port}"
        logger.debug(f"Check spark is alive: {url}")
        try:
            response = requests.get(url)
        except Exception as e:
            logger.exception(e)
            logger.warning(f"Can't access spark ui: {url}")
            return False
        return response.status_code == 200

    def exit_if_spark_dead(self):
        if not _check_spark_ui(self.logger, self.spark):
            cls_logger = getattr(self, 'logger') or logger
            cls_logger.critical("Spark is dead, exiting...")
            os.kill(os.getpid(), signal.SIGINT)

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            r = func(self, *args, **kwargs)
        finally:
            exit_if_spark_dead(self)
        return r

    return wrapper
