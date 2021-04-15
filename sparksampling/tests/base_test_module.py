from tornado.testing import AsyncHTTPTestCase
from tornado.httpclient import HTTPResponse
from sparksampling.app import make_app, debug_app
import tornado
from sparksampling.utilities.code import JSON_DECODE_ERROR

import unittest
import os
import json
import time
from json import JSONDecodeError

os.environ['ASYNC_TEST_TIMEOUT'] = '3600'


class BaseTestModule(AsyncHTTPTestCase):
    dir_fix = os.path.abspath(os.path.dirname(__file__))
    pre_fix = './requestbody/'
    test_url = r'/'

    def get_new_ioloop(self):
        return tornado.platform.asyncio.AsyncIOMainLoop()

    def get_app(self):
        return debug_app()

    def tearDown(self) -> None:
        # 间隔确保commit成功
        time.sleep(1)
        super(BaseTestModule, self).tearDown()

    def _post_data_from_file(self, filename):
        requset_data = self._get_request_data(filename)
        response = self.fetch(self.test_url, method='POST', body=requset_data, connect_timeout=0, request_timeout=0)
        data = self._get_response_data(response)
        return data

    @staticmethod
    def _get_response_data(response: HTTPResponse):
        try:
            json_data = json.loads(response.body)
        except JSONDecodeError:
            json_data = None
        return json_data

    def _get_request_data(self, file_name):
        # default: /path_to_project/tests/requestbody/{file_name}
        # modify self.pre_fix to change 'requestbody' to any path
        file_path = os.path.join(self.dir_fix, self.pre_fix, file_name)
        with open(file_path) as f:
            request_body = f.read()
        return request_body

    def _check_code(self, data, code, annotation=''):
        # response format
        # {
        #     'code': 0,
        #     'msg': '',
        #     'data': {},
        # }
        # if code is not expected,test failed and show msg
        self.assertEqual(data['code'], code, msg=data['msg'])
        print(
            f"--------- {'@' if annotation else ''} " + annotation + ' ---------\n',
            f"{self.__class__.__name__} Test Succeed: msg: {data['msg'] if data['code'] != 0 else 'Pass...'} \n"
            f"status code: {data['code']} with data:{data['data']} "
        )

    def test_json_decode_error_code(self):
        # 测试JSON解析错误时错误码抛出情况，应在子类中运行
        if self.test_url == BaseTestModule.test_url:
            return
        data = self._post_data_from_file('bad-json.json')
        self._check_code(data, JSON_DECODE_ERROR, 'Json Decode Test')


class DebugTestModule(BaseTestModule):
    def get_app(self):
        return debug_app()

    def test_hello_world(self):
        # test hello world in debug mod, make sure app is running
        # test in debug mod can show more message we need
        response = self.fetch(r'/', method='GET')
        self.assertEqual(response.code, 200)
        data = self._get_response_data(response)
        self._check_code(data, 0)


if __name__ == '__main__':
    unittest.main()
