from typing import Dict, Any
from tornado.locks import Event
from datetime import datetime

from sparksampling.core.orm import EvaluationJobTable, SampleJobTable
from sparksampling.handler.processmodule import BaseProcessModule, SamplingProcessModule, EvaluationProcessModule
from sparksampling.var import JOB_STATUS_CANCELED


class CancelBaseProcessModule(BaseProcessModule):
    sql_table = None
    job_refer_process_module: BaseProcessModule = None

    required_keys = {
        "job_id"
    }

    def __init__(self):
        super(CancelBaseProcessModule, self).__init__()

    async def process(self) -> Dict[str, Any]:
        request_data: Dict = self._request_data
        self.check_param(request_data)

        job_id = request_data.get("job_id")
        future = self.find_job_future(job_id)
        if not future:
            return self.job_not_found(job_id)

        event = Event()
        status = future.cancel()
        self.job_refer_process_module.job_list.remove((job_id, future))
        event.set()
        if not status:
            return self.cancel_job_failed(job_id)
        await self.cancel_job_in_db(job_id)
        return self.cancel_job_succeed(job_id)

    def find_job_future(self, job_id):
        job_list = self.job_refer_process_module.job_list
        for t_job_id, future in job_list:
            if t_job_id == job_id:
                return future
        return None

    def job_not_found(self, job_id):
        return {
            'code': 0,
            'msg': 'Failed: Job Not Found',
            'data': {
                "job_id": job_id,
                "is_succeed": False,
            },
        }

    def cancel_job_failed(self, job_id):
        return {
            'code': 0,
            'msg': 'Failed: Job is running',
            'data': {
                "job_id": job_id,
                "is_succeed": False,
            },
        }

    def cancel_job_succeed(self, job_id):
        return {
            'code': 0,
            'msg': 'Succeed',
            'data': {
                "job_id": job_id,
                "is_succeed": True,
            },
        }

    async def cancel_job_in_db(self, job_id):
        self.logger.info(f"Spark job {job_id} Canceled...Record job in DB...")
        async with self.sqlengine.acquire() as conn:
            async with conn.begin() as transaction:
                await conn.execute(self.sql_table.update().where(self.sql_table.c.job_id == job_id).values(
                    msg="Job Canceled",
                    status_code=JOB_STATUS_CANCELED,
                    end_time=datetime.now(),
                ))
                await transaction.commit()


class CancelSamplingJobProcessModule(CancelBaseProcessModule):
    sql_table = SampleJobTable
    job_refer_process_module = SamplingProcessModule


class CancelEvaluationJobProcessModule(CancelBaseProcessModule):
    sql_table = EvaluationJobTable
    job_refer_process_module = EvaluationProcessModule
