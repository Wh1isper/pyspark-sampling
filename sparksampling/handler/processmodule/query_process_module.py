from sparksampling.core.orm import SampleJobTable, EvaluationJobTable

from sparksampling.handler.processmodule.base_process_module import BaseQueryProcessModule
from sparksampling.utilities.var import CODE_TO_SAMPLING_METHOD_NAME, CODE_TO_JOB_STATUS, CODE_TO_EVALUATION_METHOD_NAME


class QueryJobProcessModule(BaseQueryProcessModule):
    sql_table = SampleJobTable
    required_keys = {
        'job_id',
    }

    def __init__(self):
        super(QueryJobProcessModule, self).__init__()

    def get_query_param_from_request_data(self, request_data):
        return {
            'job_id': request_data.get('job_id')
        }

    async def query(self, query_param: dict) -> dict or None:
        job_id = query_param.get('job_id')
        async with self.sqlengine.acquire() as conn:
            return await self.query_job_id(conn, job_id, self.sql_table)

    def format_response(self, response_data, details) -> dict:
        if not details:
            response_data['msg'] = 'job not found'
            return response_data
        data = {
            'job_id': details.job_id,
            'job_status': CODE_TO_JOB_STATUS[details.status_code],
            'msg': details.msg,
            'method': CODE_TO_SAMPLING_METHOD_NAME[details.method],
            'start_time': details.start_time.strftime("%Y/%m/%d/ %H:%M:%S") if details.start_time else None,
            'end_time': details.end_time.strftime("%Y/%m/%d %H:%M:%S") if details.end_time else None,
            'simpled_file_path': details.simpled_path,
            'request_data': details.request_data,
        }
        response_data['data'] = data

        return response_data


class QueryListProcessModule(BaseQueryProcessModule):
    sql_table = SampleJobTable
    required_keys = set()

    def __init__(self):
        super(QueryListProcessModule, self).__init__()

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
        if not details:
            response_data['msg'] = 'job not found'
            return response_data
        data = []
        for detail in details:
            data.append({
                'job_id': detail.job_id,
                'job_status': CODE_TO_JOB_STATUS[detail.status_code],
                'msg': detail.msg,
                'method': CODE_TO_SAMPLING_METHOD_NAME[detail.method],
                'start_time': detail.start_time.strftime("%Y/%m/%d/ %H:%M:%S") if detail.start_time else None,
                'end_time': detail.end_time.strftime("%Y/%m/%d %H:%M:%S") if detail.end_time else None,
                'simpled_file_path': detail.simpled_path,
                'request_data': detail.request_data,
            })
        response_data['data'] = data

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
        if not details:
            response_data['msg'] = 'job not found'
            return response_data
        data = []
        for detail in details:
            data.append({
                'job_id': detail.job_id,
                'job_status': CODE_TO_JOB_STATUS[detail.status_code],
                'msg': detail.msg,
                'method': CODE_TO_EVALUATION_METHOD_NAME[detail.method],
                'start_time': detail.start_time.strftime("%Y/%m/%d/ %H:%M:%S") if detail.start_time else None,
                'end_time': detail.end_time.strftime("%Y/%m/%d %H:%M:%S") if detail.end_time else None,
                'result': detail.result,
                'request_data': detail.request_data,
            })
        response_data['data'] = data

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
        if not details:
            response_data['msg'] = 'job not found'
            return response_data
        data = {
            'job_id': details.job_id,
            'job_status': CODE_TO_JOB_STATUS[details.status_code],
            'msg': details.msg,
            'method': CODE_TO_EVALUATION_METHOD_NAME[details.method],
            'start_time': details.start_time.strftime("%Y/%m/%d/ %H:%M:%S") if details.start_time else None,
            'end_time': details.end_time.strftime("%Y/%m/%d %H:%M:%S") if details.end_time else None,
            'result': details.result,
            'request_data': details.request_data,
        }
        response_data['data'] = data

        return response_data
