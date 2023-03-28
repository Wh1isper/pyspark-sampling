BAD_PARAM_ERROR = 4000
PROCESS_ERROR = 4004
PROCESS_PRE_HOOK_ERROR = 4011
PROCESS_POST_HOOK_ERROR = 4012
EXHAUSTED_ERROR = 4006
SERVER_ERROR = 5000


class CustomErrorWithCode(Exception):
    def __init__(self, code, error_info):
        super().__init__(self)
        self.code = code
        self.errorinfo = error_info

    def __str__(self):
        return self.errorinfo

    def error_response(self):
        return {
            "code": self.code,
            "message": self.errorinfo,
        }


class ExhaustedError(CustomErrorWithCode):
    def __init__(self, msg: str):
        super(ExhaustedError, self).__init__(EXHAUSTED_ERROR, msg)


class BadParamError(CustomErrorWithCode):
    def __init__(self, msg: str):
        super(BadParamError, self).__init__(BAD_PARAM_ERROR, msg)


class ProcessError(CustomErrorWithCode):
    def __init__(self, msg: str):
        super(ProcessError, self).__init__(PROCESS_ERROR, msg)


class ProcessPreHookError(CustomErrorWithCode):
    def __init__(self, msg: str):
        super(ProcessPreHookError, self).__init__(PROCESS_PRE_HOOK_ERROR, msg)


class ProcessPostHookError(CustomErrorWithCode):
    def __init__(self, msg: str):
        super(ProcessPostHookError, self).__init__(PROCESS_POST_HOOK_ERROR, msg)
