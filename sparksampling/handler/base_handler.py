"""
handler基类
"""
import json
import sys
import logging
import traceback
from tornado.web import RequestHandler

from json import JSONDecodeError

from sparksampling.handler.processmodule.base_process_module import BaseProcessModule
from sparksampling.utilities.code import NORMAL_FAILD as NOT_FOUND_FAIL
from sparksampling.utilities.code import PROCESS_ERROR
from sparksampling.utilities import CustomErrorWithCode


class BaseProcessHandler(RequestHandler):
    """
    通用处理框架，接收post方法，调用fetch进行操作
    每次执行都将初始化ProcessModule，如果任务足够简单，只需初始化一次，请使用SingletonHandler
    """
    logger = logging.getLogger('SAMPLING')

    def initialize(self, processmodule: type = BaseProcessModule):
        self.processmodule = processmodule()

    def prepare(self):
        self.logger.info(f'-----{self.request.path} from {self.request.remote_ip} begin -----')
        self.set_header('Content-Type', 'application/json;')

    def on_finish(self) -> None:
        self.logger.info(f'-----{self.request.path}@{self.request.remote_ip} finished -----')

    async def fetch(self, data=None, kw=None):
        # 通用处理框架，接受请求、交给ProcessModule处理
        # ProcessModule应根据需求处理意料中的错误，并返回4XX
        # 若ProcessModule出错，则由此模块交由write_error函数处理，返回5XX
        response_data = {
            'code': NOT_FOUND_FAIL,
            'msg': "process not found",
            'data': {}
        }
        # process or return 404
        if self.processmodule is not None:
            try:
                # 在handler层面就尝试对json进行解析，作为通用框架，解析失败将传递原文
                # 如果使用BaseProcessHandler接受请求，需要在ProcessModule内验证数据是否符合要求
                json_data = json.loads(data)
                self.logger.info(f"request data : {json_data}")
                await self.processmodule.prepare(json_data, kw)
            except CustomErrorWithCode as e:
                self.logger.error(f"{type(self.processmodule).__name__} Process Error: {str(e)}")
                response_data = e.error_response()
                return json.dumps(response_data)
            except JSONDecodeError:
                self.logger.warning(f"Request body JSON Decode Failed, data : {data}")
                await self.processmodule.prepare(data, kw)

            try:
                return_data = await self.processmodule.process()
                if return_data is not None:
                    response_data = return_data
            except NotImplementedError as e:
                self.logger.error(e)
                raise
            except CustomErrorWithCode as e:
                self.logger.error(f"{type(self.processmodule).__name__} Process Error: {str(e)}")
                response_data = e.error_response()
            except:
                # 抛给 write_error 处理
                self.logger.error("Process Error...")
                raise
        else:
            self.set_status(404)

        return json.dumps(response_data)

    def write_error(self, status_code, **kwargs):
        if status_code == 500:
            excp = kwargs['exc_info'][1]
            tb = kwargs['exc_info'][2]
            stack = traceback.extract_tb(tb)
            clean_stack = [i for i in stack if i[0][-6:] != 'gen.py' and i[0][-13:] != 'concurrent.py']
            error_msg = '{}\n  Exception: {}'.format(''.join(traceback.format_list(clean_stack)), excp)

            self.logger.error(error_msg)
            response = {
                'code': PROCESS_ERROR,
                'msg': "Process error...Contact the developer if you think this shouldn't happen",
                'data': {}

            }
            self.write(json.dumps(response))
            self.finish()

    async def post(self, kw=None):
        # 请求入口，由于pyspark的限制，一般来说在生成job之后就将run_job
        try:
            result = await self.fetch(self.request.body, kw)
            self.write(result)
            await self.finish()
            await self.processmodule.run_job()
        except Exception:
            self.send_error(500, exc_info=sys.exc_info())
