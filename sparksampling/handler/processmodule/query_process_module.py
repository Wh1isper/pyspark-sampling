from typing import Dict, Any

from sparksampling.core.orm import SampleJobTable, EvaluationJobTable

from sparksampling.handler.processmodule import BaseProcessModule
from sparksampling.utilities import JsonDecodeError, SQLError
from sparksampling.var import CODE_TO_SAMPLING_METHOD_NAME, CODE_TO_JOB_STATUS, CODE_TO_EVALUATION_METHOD_NAME


class BaseQueryProcessModule(BaseProcessModule):
    sql_table = None
    MSG_JOB_NOT_FOUND = 'Job not found'

    def __init__(self):
        super(BaseQueryProcessModule, self).__init__()

    def get_query_param_from_request_data(self, request_data):
        raise NotImplementedError

    @staticmethod
    async def query_job_id(conn, job_id, table):
        result = await conn.execute(table.select().where(table.c.job_id == job_id))
        details = await result.fetchone()
        return details

    async def query(self, query_param) -> dict or None:
        raise NotImplementedError

    def format_response(self, response_data, details) -> dict:
        raise NotImplementedError

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
        if not details:
            return self.response_job_not_found(response_data)
        response_data = self.format_response(response_data, details)
        return response_data

    def response_job_not_found(self, response_data):
        response_data['msg'] = self.MSG_JOB_NOT_FOUND
        return response_data


class QuerySamplingJobProcessModule(BaseQueryProcessModule):
    sql_table = SampleJobTable
    required_keys = {
        'job_id',
    }

    def __init__(self):
        super(QuerySamplingJobProcessModule, self).__init__()

    def get_query_param_from_request_data(self, request_data):
        return {
            'job_id': request_data.get('job_id')
        }

    async def query(self, query_param: dict) -> dict or None:
        job_id = query_param.get('job_id')
        async with self.sqlengine.acquire() as conn:
            return await self.query_job_id(conn, job_id, self.sql_table)

    def format_response(self, response_data, details) -> dict:
        data = {
            'job_id': details.job_id,
            'job_status': CODE_TO_JOB_STATUS.get(details.status_code, details.status_code),
            'msg': details.msg,
            'method': CODE_TO_SAMPLING_METHOD_NAME.get(details.method, details.method),
            'start_time': details.start_time.strftime("%Y/%m/%d/ %H:%M:%S") if details.start_time else None,
            'end_time': details.end_time.strftime("%Y/%m/%d %H:%M:%S") if details.end_time else None,
            'simpled_file_path': details.simpled_path,
            'request_data': details.request_data,
        }
        response_data['data'] = data

        return response_data


class QuerySamplingListProcessModule(BaseQueryProcessModule):
    sql_table = SampleJobTable
    required_keys = set()

    def __init__(self):
        super(QuerySamplingListProcessModule, self).__init__()

    def get_query_param_from_request_data(self, request_data):
        return {
            'offset': request_data.get('offset', 0),
            'limit': request_data.get('limit', 20)
        }

    async def query(self, query_param) -> dict or None:
        offset = query_param.get('offset')
        limit = query_param.get('limit')
        async with self.sqlengine.acquire() as conn:
            result = await conn.execute(self.sql_table.select().limit(limit).offset(offset))
            details = await result.fetchall()
        return details

    def format_response(self, response_data, details) -> dict:
        data = []
        for detail in details:
            data.append({
                'job_id': detail.job_id,
                'job_status': CODE_TO_JOB_STATUS.get(detail.status_code, detail.status_code),
                'msg': detail.msg,
                'method': CODE_TO_SAMPLING_METHOD_NAME.get(detail.method, detail.method),
                'start_time': detail.start_time.strftime("%Y/%m/%d/ %H:%M:%S") if detail.start_time else None,
                'end_time': detail.end_time.strftime("%Y/%m/%d %H:%M:%S") if detail.end_time else None,
            })
        response_data['data'] = {'query_result': data}

        return response_data


class QueryEvaluationListProcessModule(BaseQueryProcessModule):
    sql_table = EvaluationJobTable
    required_keys = set()

    def __init__(self):
        super(QueryEvaluationListProcessModule, self).__init__()

    def get_query_param_from_request_data(self, request_data):
        return {
            'offset': request_data.get('offset', 0),
            'limit': request_data.get('limit', 20)
        }

    async def query(self, query_param) -> dict or None:
        offset = query_param.get('offset')
        limit = query_param.get('limit')
        async with self.sqlengine.acquire() as conn:
            result = await conn.execute(self.sql_table.select().limit(limit).offset(offset))
            details = await result.fetchall()
        return details

    def format_response(self, response_data, details) -> dict:
        data = []
        for detail in details:
            data.append({
                'job_id': detail.job_id,
                'job_status': CODE_TO_JOB_STATUS.get(detail.status_code, detail.status_code),
                'msg': detail.msg,
                'method': CODE_TO_EVALUATION_METHOD_NAME.get(detail.method, detail.method),
                'start_time': detail.start_time.strftime("%Y/%m/%d/ %H:%M:%S") if detail.start_time else None,
                'end_time': detail.end_time.strftime("%Y/%m/%d %H:%M:%S") if detail.end_time else None,
            })
        response_data['data'] = {'query_result': data}

        return response_data


class QueryEvaluationJobProcessModule(BaseQueryProcessModule):
    sql_table = EvaluationJobTable
    required_keys = {
        'job_id',
    }

    def __init__(self):
        super(QueryEvaluationJobProcessModule, self).__init__()

    def get_query_param_from_request_data(self, request_data):
        return {
            'job_id': request_data.get('job_id')
        }

    async def query(self, query_param: dict) -> dict or None:
        job_id = query_param.get('job_id')
        async with self.sqlengine.acquire() as conn:
            return await self.query_job_id(conn, job_id, self.sql_table)

    def format_response(self, response_data, details) -> dict:
        data = {
            'job_id': details.job_id,
            'job_status': CODE_TO_JOB_STATUS.get(details.status_code, details.status_code),
            'msg': details.msg,
            'method': CODE_TO_EVALUATION_METHOD_NAME.get(details.method, details.method),
            'start_time': details.start_time.strftime("%Y/%m/%d/ %H:%M:%S") if details.start_time else None,
            'end_time': details.end_time.strftime("%Y/%m/%d %H:%M:%S") if details.end_time else None,
            'result': details.result,
            'request_data': details.request_data,
        }
        response_data['data'] = data

        return response_data
