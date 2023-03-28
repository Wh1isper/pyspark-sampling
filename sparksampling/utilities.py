import logging
import os
import re
import signal
import time
from functools import partial, wraps

import requests
from google.protobuf.json_format import MessageToDict
from pyspark.sql import SparkSession

from sparksampling.error import SERVER_ERROR, CustomErrorWithCode

logger = logging.getLogger("sparksampling")


def hump2underline(hunp_str):
    p = re.compile(r"([a-z]|\d)([A-Z])")
    sub = re.sub(p, r"\1_\2", hunp_str).lower()
    return sub


def hump2underline_dict(data):
    keys = list(data.keys())
    for key in keys:
        underline_key = hump2underline(key)
        if type(data[key]) is dict:
            data[underline_key] = hump2underline_dict(data[key])
        data[underline_key] = data.pop(key)
    return data


def underline2hump(underline_str):
    sub = re.sub(r"(_\w)", lambda x: x.group(1)[1].upper(), underline_str)
    return sub


def throw_exception(func=None, *, request_type=None, response_type=None):
    if func is None:
        return partial(throw_exception, request_type=request_type, response_type=response_type)

    if not request_type:
        request_type = func.__annotations__["request"]

    if not response_type:
        response_type = func.__annotations__["return"]

    @wraps(func)
    def wrapper(self, request, context):
        data = MessageToDict(request, preserving_proto_field_name=True)
        parent_request = request_type(**data)
        start_time = time.time()
        try:
            return func(self, request, context)
        except CustomErrorWithCode as e:
            logger.exception(e)
            return response_type(
                **e.error_response(), data=response_type.ResponseData(parent_request=parent_request)
            )
        except Exception as e:
            logger.exception(e)
            return response_type(
                code=SERVER_ERROR,
                message=str(e),
                data=response_type.ResponseData(parent_request=parent_request),
            )
        finally:
            end_time = time.time()
            cls_logger = getattr(self, "logger", logger)
            cls_logger.debug(f"execute in {str(end_time - start_time).split('.')[0]} seconds")

    return wrapper


def async_throw_exception(func=None, *, request_type=None, response_type=None):
    if func is None:
        return partial(throw_exception, request_type=request_type, response_type=response_type)

    if not request_type:
        request_type = func.__annotations__["request"]

    if not response_type:
        response_type = func.__annotations__["return"]

    @wraps(func)
    async def wrapper(self, request, context):
        data = MessageToDict(request, preserving_proto_field_name=True)
        parent_request = request_type(**data)
        start_time = time.time()
        try:
            return await func(self, request, context)
        except CustomErrorWithCode as e:
            logger.exception(e)
            return response_type(
                **e.error_response(), data=response_type.ResponseData(parent_request=parent_request)
            )
        except Exception as e:
            logger.exception(e)
            return response_type(
                code=SERVER_ERROR,
                message=str(e),
                data=response_type.ResponseData(parent_request=parent_request),
            )
        finally:
            end_time = time.time()
            cls_logger = getattr(self, "logger", logger)
            cls_logger.debug(f"execute in {str(end_time - start_time).split('.')[0]} seconds")

    return wrapper


def check_spark_session(func):
    # Check the spark driver status and exit if it is not responding
    # Server will be redeployed under K8S deployment
    def _check_spark_ui(logger, spark: SparkSession):
        try:
            if not spark.conf.get("spark.submit.deployMode") == "client":
                return True
        except Exception as e:
            logger.exception(e)
            logger.warning(f"can't access spark.conf")
            return False

        ui_port = spark.conf.get("spark.ui.port")
        url = f"http://localhost:{ui_port}"
        logger.debug(f"Check Spark is alive: {url}")
        try:
            response = requests.get(url)
        except Exception as e:
            logger.exception(e)
            logger.warning(f"Can't access Spark ui: {url}")
            return False
        return response.status_code == 200

    def exit_if_spark_dead(self):
        if not _check_spark_ui(self.logger, self.spark):
            cls_logger = getattr(self, "logger") or logger
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
