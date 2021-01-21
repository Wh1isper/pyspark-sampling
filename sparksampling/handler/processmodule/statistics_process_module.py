from sparksampling.core.orm import SampleJobTable
from sparksampling.core.sampling.engine.statistics_engine import EvaluationEngine
from sparksampling.handler.processmodule import BaseProcessModule
from typing import Dict, Any

from sparksampling.utilities.custom_error import JobProcessError
from sparksampling.utilities.var import JOB_STATUS_SUCCEED


class StatisticsProcessModule(BaseProcessModule):
    sql_table = SampleJobTable
    required_keys = {
        'method',
        'type',
    }

    def __init__(self):
        super(StatisticsProcessModule, self).__init__()

    async def process(self) -> Dict[str, Any]:
        response_data = {
            'code': 0,
            'msg': "",
            'data': {}
        }
        request_data: Dict = self._request_data
        self.check_param(request_data)
        conf = await self.format_conf(request_data)
        if not conf:
            raise JobProcessError("Job status is not succeed, can't run statistics")
        engine = self.config_engine(conf)
        response_data['data'] = engine.submit()
        return response_data

    def config_engine(self, conf) -> EvaluationEngine:
        self.logger.info(f"Config Engine with conf: {conf}")
        return EvaluationEngine(**conf)

    async def format_conf(self, request_data):
        from_sampling = request_data.get('from_sampling')
        job_id = request_data.get('job_id')
        if from_sampling:
            self.logger.info(f"Run statistics for sampling job {job_id}")
            is_succeed, path = await self.get_path_from_sampling_job(job_id)
            self.logger.info(f"Get sampled file path {path}")
            if not is_succeed:
                return False
        else:
            path = request_data.get('path')
            self.logger.info(f"Run statistics for file {path}")
        return {
            'path': path,
            'method': request_data.get('method', 1),
            'file_type': request_data.get('type', 2),
            'with_header': request_data.get('with_header', True)
        }

    async def get_path_from_sampling_job(self, job_id):
        async with self.sqlengine.acquire() as conn:
            result = await conn.execute(self.sql_table.select().where(self.sql_table.c.job_id == job_id))
            details = await result.fetchone()
        return details.status_code == JOB_STATUS_SUCCEED, details.simpled_path
