from sparksampling.core.engine import EvaluationEngine
from sparksampling.handler.processmodule import BaseProcessModule
from typing import Dict, Any

from sparksampling.handler.processmodule.base_process_module import BaseQueryProcessModule
from sparksampling.utilities import TypeCheckError
from sparksampling.utilities.var import JOB_STATUS_SUCCEED, JOB_STATUS_PADDING
from sparksampling.utilities.var import EVALUATION_COMPARE_METHOD, FILE_TYPE_TEXT
from sparksampling.utilities.var import JOB_CANCELED, JOB_CREATED, JOB_CREATING
from sparksampling.utilities import CustomErrorWithCode
from sparksampling.utilities.utilities import convert_dict_value_to_string_value
from sparksampling.core.orm import EvaluationJobTable

from datetime import datetime


class EvaluationProcessModule(BaseProcessModule):
    sql_table = EvaluationJobTable

    required_keys = {
        'path',
        'method',
    }

    def __init__(self):
        super(EvaluationProcessModule, self).__init__()
        self.job_id = None
        self.sample_engine = None
        self.job_stats = JOB_CREATING

    async def process(self) -> Dict[str, Any]:
        """
        配置评估任务
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
        if conf.get('compare_job_id'):
            path, source_path = await self.get_job_path(conf.get('compare_job_id'))
            conf.update(**{
                'path': path,
                'source_path': source_path,
            })
        try:
            response_data['data'] = await self.create_job(conf)
            self.job_stats = JOB_CREATED
        except TypeError as e:
            self.job_stats = JOB_CANCELED
            self.logger.info(f"Create job failed: {e}")
            raise TypeCheckError(str(e))
        return response_data

    def format_conf(self, request_data: Dict):
        return {
            'path': request_data.get('path'),
            'source_path': request_data.get('source_path'),
            'compare_job_id': request_data.get('compare_job_id'),
            'method': request_data.get('method', EVALUATION_COMPARE_METHOD),
            'file_type': request_data.get('type', FILE_TYPE_TEXT),
            'with_header': request_data.get('with_header', True),
        }

    async def get_job_path(self, job_id):
        async with self.sqlengine.acquire() as conn:
            details = await BaseQueryProcessModule.query_job_id(conn, job_id, self.sql_table)
        return details.simpled_path, details.path

    async def run_job(self):
        try:
            result = self.sample_engine.submit(self.job_id, df_output=False)
            await self.finish_job(result)
        except CustomErrorWithCode as e:
            await self.error_job(e)

    async def create_job(self, conf):
        self.sample_engine = self.config_engine(conf)
        self.job_id = await self.init_job(conf)
        self.logger.info(f"Finish create job, job_id: {self.job_id}")
        return {
            'job_id': self.job_id
        }

    def config_engine(self, conf) -> EvaluationEngine:
        return EvaluationEngine(**conf)

    async def init_job(self, conf):
        self.logger.info("Store Spark job conf into DB...")
        async with self.sqlengine.acquire() as conn:
            convert_dict_value_to_string_value(conf)
            await conn.execute(self.sql_table.insert().values(start_time=datetime.now(),
                                                              path=conf.get('path'),
                                                              source_path=conf.get('source_path'),
                                                              method=conf.get('method'),
                                                              request_data=str(self._request_data),
                                                              status_code=JOB_STATUS_PADDING
                                                              ))
            result = await conn.execute("select @@IDENTITY")
            job_id = (await result.fetchone())[0]
            await conn._commit_impl()
        return job_id

    async def finish_job(self, result):
        if not self.is_job_created:
            return
        self.logger.info(f"Spark job {self.job_id} finished...Record job in DB...")
        async with self.sqlengine.acquire() as conn:
            await conn.execute(self.sql_table.update().where(self.sql_table.c.job_id == self.job_id).values(
                msg='succeed',
                status_code=JOB_STATUS_SUCCEED,
                end_time=datetime.now(),
                result=result
            ))
            await conn._commit_impl()

    async def error_job(self, e: CustomErrorWithCode):
        self.logger.info(f"Spark job {self.job_id} failed...Record job in DB...")
        async with self.sqlengine.acquire() as conn:
            await conn.execute(self.sql_table.update().where(self.sql_table.c.job_id == self.job_id).values(
                msg=e.errorinfo,
                status_code=e.code,
                end_time=datetime.now(),
            ))
            await conn._commit_impl()