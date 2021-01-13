from sparksampling.handler.processmodule import BaseProcessModule
from sparksampling.core.orm import SampleJobTable
from typing import Dict, Any

from sparksampling.utilities import JsonDecodeError
from sparksampling.utilities.custom_error import SQLError
from sparksampling.utilities.var import CODE_TO_METHOD_NAME, CODE_TO_JOB_STATUS


class QueryProcessModule(BaseProcessModule):
    sql_table = SampleJobTable
    required_keys = {
        'job_id',
    }

    def __init__(self):
        super(QueryProcessModule, self).__init__()

    async def process(self) -> Dict[str, Any]:
        response_data = {
            'code': 0,
            'msg': "",
            'data': {}
        }
        request_data: Dict = self._request_data

        if type(request_data) is not dict:
            raise JsonDecodeError
        self.check_param(request_data)
        query_param = self.get_query_param_from_request_data(request_data)
        try:
            details = await self.query(query_param)
        except Exception as e:
            raise SQLError(str(e))
        response_data = self.format_response(response_data, details)
        return response_data

    def get_query_param_from_request_data(self, request_data):
        return {
            'job_id': request_data.get('job_id')
        }

    async def query(self, query_param: dict) -> dict or None:
        job_id = query_param.get('job_id')
        async with self.sqlengine.acquire() as conn:
            result = await conn.execute(self.sql_table.select().where(self.sql_table.c.job_id == job_id))
            print(self.sql_table.select().where(self.sql_table.c.job_id == job_id))
            details = await result.fetchone()
        return details

    def format_response(self, response_data, details) -> dict:
        if not details:
            response_data['msg'] = 'job not found'
            return response_data
        data = {
            'job_id': details.job_id,
            'job_status': CODE_TO_JOB_STATUS[details.status_code],
            'msg': details.msg,
            'method': CODE_TO_METHOD_NAME[details.method],
            'start_time': details.start_time.strftime("%Y/%m/%d/ %H:%M:%S") if details.start_time else None,
            'end_time': details.end_time.strftime("%Y/%m/%d %H:%M:%S") if details.end_time else None,
            'simpled_file_path': details.simpled_path,
            'request_data': details.request_data,
        }
        response_data['data'] = data

        return response_data
