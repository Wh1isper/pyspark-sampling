"""
自定义错误类
"""
class CustomErrorWithCode(Exception):
    def __init__(self, code, error_info):
        super().__init__(self)  # 初始化父类
        self.errorinfo = error_info
        self.code = code

    def __str__(self):
        return self.errorinfo

    def error_response(self):
        return {
            'code': self.code,
            'msg': self.errorinfo,
            'data': {},
        }
