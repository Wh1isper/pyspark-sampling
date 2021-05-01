from typing import Dict, Any
import random
from datetime import datetime
from tornado import ioloop
from sparksampling.utilities import TypeCheckError
from sparksampling.core.engine import SamplingEngine
from sparksampling.var import JOB_STATUS_SUCCEED, JOB_STATUS_PADDING
from sparksampling.var import SIMPLE_RANDOM_SAMPLING_METHOD, FILE_TYPE_TEXT
from sparksampling.var import JOB_CANCELED, JOB_CREATED, JOB_CREATING
from sparksampling.utilities import CustomErrorWithCode
from sparksampling.utilities.utilities import convert_dict_value_to_string_value
from sparksampling.core.orm import SampleJobTable
from sparksampling.handler.processmodule import BaseProcessModule


class SamplingProcessModule(BaseProcessModule):
    sql_table = SampleJobTable

    required_keys = {
        'path',
        'method',
    }

    def __init__(self):
        super(SamplingProcessModule, self).__init__()
        self.job_id = None
        self.sample_engine = None
        self.job_stats = JOB_CREATING

    async def process(self) -> Dict[str, Any]:
        """
        配置抽样任务
        create_job用于生成抽样任务，process在生成抽样任务后就会返回
        上级决定何时调用run_job运行抽样任务
        Returns:

        """
        response_data = {
            'code': 0,
            'msg': "",
            'data': {}
        }
        request_data: Dict = self._request_data
        self.check_param(request_data)

        conf = self.format_conf(request_data)
        try:
            response_data['data'] = await self.create_job(conf)
            self.job_stats = JOB_CREATED
        except TypeError as e:
            self.job_stats = JOB_CANCELED
            self.logger.info(f"Create job failed: {e}")
            raise TypeCheckError(str(e))
        return response_data

    def format_conf(self, request_data: Dict):
        conf = request_data.get('conf', dict())
        formatted = conf
        formatted.update(self.base_conf(request_data))
        formatted.update(self.job_conf(conf))
        return formatted

    def base_conf(self, request_data):
        return {
            'path': request_data.get('path'),
            'method': request_data.get('method', SIMPLE_RANDOM_SAMPLING_METHOD),
            'file_type': request_data.get('type', FILE_TYPE_TEXT),
            'with_header': request_data.get('with_header', True),
        }

    def job_conf(self, conf):
        job_conf = self.__random_job_conf(conf)
        job_conf.update(self.__stratified_job_conf(conf))
        return job_conf

    def __random_job_conf(self, conf):
        return {
            'fraction': conf.get('fraction', 0.5),
            'seed': conf.get('seed', random.randint(1, 65535)),
            'with_replacement': conf.get('with_replacement', True)
        }

    def __stratified_job_conf(self, conf):
        return {
            'col_key': conf.get('key')
        }

    def config_engine(self, conf) -> SamplingEngine:
        return SamplingEngine(**conf)

    async def run_job(self):
        try:
            future = ioloop.IOLoop.current().run_in_executor(self.executor, self.sample_engine.submit,
                                                             self.job_id)
            SamplingProcessModule.job_list.append((self.job_id, future))
            self.logger.info(f"Sampling Job Enqueued: {self.job_id}")
            new_path = await future
            if (self.job_id, future) in SamplingProcessModule.job_list:
                SamplingProcessModule.job_list.remove((self.job_id, future))
            await self.finish_job(new_path)
        except CustomErrorWithCode as e:
            await self.error_job(e)

    async def create_job(self, conf):
        self.sample_engine = self.config_engine(conf)
        self.job_id = await self.init_job(conf)
        self.logger.info(f"Finish create job, job_id: {self.job_id}")
        return {
            'job_id': self.job_id
        }

    async def init_job(self, conf):
        self.logger.info("Store Spark job conf into DB...")
        async with self.sqlengine.acquire() as conn:
            convert_dict_value_to_string_value(conf)
            async with conn.begin() as transaction:
                await conn.execute(self.sql_table.insert().values(start_time=datetime.now(),
                                                                  path=conf.get('path'),
                                                                  method=conf.get('method'),
                                                                  request_data=str(self._request_data),
                                                                  status_code=JOB_STATUS_PADDING
                                                                  ))
                result = await conn.execute("select @@IDENTITY")
                job_id = (await result.fetchone())[0]
                await transaction.commit()
        return job_id

    async def finish_job(self, new_path):
        if not self.is_job_created:
            return
        self.logger.info(f"Spark job {self.job_id} finished...Record job in DB...")
        async with self.sqlengine.acquire() as conn:
            async with conn.begin() as transaction:
                await conn.execute(self.sql_table.update().where(self.sql_table.c.job_id == self.job_id).values(
                    msg='succeed',
                    status_code=JOB_STATUS_SUCCEED,
                    end_time=datetime.now(),
                    simpled_path=new_path
                ))
                await transaction.commit()

    async def error_job(self, e: CustomErrorWithCode):
        self.logger.info(f"Spark job {self.job_id} failed...Record job in DB...")
        async with self.sqlengine.acquire() as conn:
            async with conn.begin() as transaction:
                await conn.execute(self.sql_table.update().where(self.sql_table.c.job_id == self.job_id).values(
                    msg=e.errorinfo,
                    status_code=e.code,
                    end_time=datetime.now(),
                ))
                await transaction.commit()
