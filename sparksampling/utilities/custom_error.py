"""
自定义错误类
"""
from sparksampling.utilities.code import JSON_DECODE_ERROR, TYPE_ERROR, BAD_PARAM_ERROR, SQL_ERROR
from sparksampling.utilities.var import JOB_STATUS_KEY_ERROR, JOB_STATUS_PROCESS_ERROR, JOB_STATUS_TYPE_ERROR


class CustomErrorWithCode(Exception):
    def __init__(self, code, error_info):
        super().__init__(self)  # 初始化父类
        self.code = code
        self.errorinfo = error_info

    def __str__(self):
        return self.errorinfo

    def error_response(self):
        return {
            'code': self.code,
            'msg': self.errorinfo,
            'data': {},
        }


class JsonDecodeError(CustomErrorWithCode):
    def __init__(self):
        super(JsonDecodeError, self).__init__(JSON_DECODE_ERROR, "Json decode error. Data must be json.")


class TypeCheckError(CustomErrorWithCode):
    def __init__(self, msg: str):
        super(TypeCheckError, self).__init__(TYPE_ERROR, msg)


class SQLError(CustomErrorWithCode):
    def __init__(self, msg: str):
        super(SQLError, self).__init__(SQL_ERROR, msg)


class BadParamError(CustomErrorWithCode):
    def __init__(self, msg: str):
        super(BadParamError, self).__init__(BAD_PARAM_ERROR, msg)


class JobTypeError(CustomErrorWithCode):
    def __init__(self, msg: str):
        super(JobTypeError, self).__init__(JOB_STATUS_TYPE_ERROR, msg)


class JobKeyError(CustomErrorWithCode):
    def __init__(self, msg: str):
        super(JobKeyError, self).__init__(JOB_STATUS_KEY_ERROR, msg)


class JobProcessError(CustomErrorWithCode):
    def __init__(self, msg: str):
        super(JobProcessError, self).__init__(JOB_STATUS_PROCESS_ERROR, msg)
