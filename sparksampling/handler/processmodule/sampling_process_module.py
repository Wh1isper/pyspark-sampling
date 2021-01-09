from sparksampling.handler.processmodule import BaseProcessModule
from typing import Dict, Any
import random
from sparksampling.utilities import JsonDecodeError, TypeCheckError
from sparksampling.core import SamplingEngine
from sparksampling.utilities.var import SIMPLE_RANDOM_SAMPLING_METHOD, FILE_TYPE_TEXT
from sparksampling.utilities.var import JOB_CANCELED, JOB_CREATED, JOB_CREATING
from sparksampling.utilities.utilities import convert_dict_value_to_string_value
from sparksampling.core.orm import SampleJobTable
from datetime import datetime


class SamplingProcessModule(BaseProcessModule):
    required_keys = {
        'path'
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

        if type(request_data) is not dict:
            raise JsonDecodeError
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
        return {
            'path': request_data.get('path'),
            'method': request_data.get('method', SIMPLE_RANDOM_SAMPLING_METHOD),
            'fraction': conf.get('fraction', '0.5'),
            'file_type': conf.get('type', FILE_TYPE_TEXT),
            'with_header': bool(conf.get('with_header', True)),
            'seed': int(conf.get('seed', random.randint(1, 65535))),
            'col_key': conf.get('key')
        }

    def config_engine(self, conf) -> SamplingEngine:
        return SamplingEngine(**conf)

    async def run_job(self):
        try:
            new_path = self.sample_engine.submit()
        except Exception as e:
            await self.error_job(str(e))
            return
        await self.finish_job(new_path)

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
            await conn.execute(SampleJobTable.insert().values(start_time=datetime.now(), **conf))
            result = await conn.execute("select @@IDENTITY")
            job_id = (await result.fetchone())[0]
            await conn._commit_impl()
        return job_id

    async def finish_job(self, new_path):
        if not self.is_job_created:
            return
        self.logger.info("Spark job finished...Record job in DB...")
        async with self.sqlengine.acquire() as conn:
            await conn.execute(SampleJobTable.update().where(SampleJobTable.c.job_id == self.job_id).values(
                msg='succeed',
                end_time=datetime.now(),
                simpled_path=new_path
            ))
            await conn._commit_impl()

    async def error_job(self, msg):
        self.logger.info("Spark job failed...Record job in DB...")
        async with self.sqlengine.acquire() as conn:
            await conn.execute(SampleJobTable.update().where(SampleJobTable.c.job_id == self.job_id).values(
                msg=msg,
                end_time=datetime.now(),
            ))
            await conn._commit_impl()
