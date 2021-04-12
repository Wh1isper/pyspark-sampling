from sparksampling.core.orm import SampleJobTable
from sparksampling.core.engine import StatisticsEngine
from sparksampling.handler.processmodule import BaseProcessModule
from typing import Dict, Any

from sparksampling.handler.processmodule.base_process_module import BaseQueryProcessModule
from sparksampling.utilities.custom_error import JobProcessError
from sparksampling.var import JOB_STATUS_SUCCEED


class StatisticsProcessModule(BaseProcessModule):
    sql_table = SampleJobTable
    required_keys = {
        'method',
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
        conf = request_data
        format_conf = await self.format_conf(request_data)
        if not conf:
            raise JobProcessError("Job status is not succeed or can not find file, can't run statistics")
        conf.update(format_conf)
        engine = self.config_engine(conf)
        response_data['data'] = engine.submit(df_output=False)
        return response_data

    def config_engine(self, conf) -> StatisticsEngine:
        self.logger.info(f"Config Engine with conf: {conf}")
        return StatisticsEngine(**conf)

    async def format_conf(self, request_data):
        job_id = request_data.get('job_id')
        if job_id:
            self.logger.info(f"Run statistics for sampling job {job_id}")
            is_succeed, path = await self.get_path_from_sampling_job(job_id)
            self.logger.info(f"Get sampled file path {path}")
            if not is_succeed:
                return False
        else:
            path = request_data.get('path')
            self.logger.info(f"Run statistics for file {path}")
            if not path:
                return False
        return {
            'path': path,
            'method': request_data.get('method', 1),
            'file_type': request_data.get('type', 2),
            'with_header': request_data.get('with_header', True)
        }

    async def get_path_from_sampling_job(self, job_id):
        async with self.sqlengine.acquire() as conn:
            details = await BaseQueryProcessModule.query_job_id(conn, job_id, self.sql_table)
        return (details.status_code == JOB_STATUS_SUCCEED, details.simpled_path) if details else (False, None)
