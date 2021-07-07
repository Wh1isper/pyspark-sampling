from typing import Dict, Any

from pyspark.sql import SparkSession

from sparksampling.core.orm import SampleJobTable, EvaluationJobTable
from sparksampling.handler.processmodule import BaseProcessModule
from sparksampling.utilities import CustomErrorWithCode
from sparksampling.config import TOKEN, get_spark_conf

from tornado.locks import Event
from sparksampling.core.engine.base_engine import BaseEngine
from sparksampling.var import JOB_STATUS_PADDING, JOB_STATUS_PROCESS_ERROR


class StopProcessModule(BaseProcessModule):
    sql_tables = [SampleJobTable, EvaluationJobTable]
    required_keys = {
        'token'
    }

    def __init__(self):
        super(StopProcessModule, self).__init__()

    def check_param(self, request_data):
        super(StopProcessModule, self).check_param(request_data)
        token = request_data.get('token')
        if token != TOKEN:
            raise ValueError("token mismatch")

    async def process(self) -> Dict[str, Any]:
        response_data = {
            'code': 0,
            'msg': 'Server Stoped',
            'data': {},
        }
        event = Event()
        try:
            async with self.sqlengine.acquire() as conn:
                async with conn.begin() as transaction:
                    for sql_table in self.sql_tables:
                        await conn.execute(
                            sql_table.update().where(sql_table.c.status_code == JOB_STATUS_PADDING).values(
                                status_code=JOB_STATUS_PROCESS_ERROR, msg='Spark Crushed'))
                    await transaction.commit()
        except CustomErrorWithCode as e:
            # catch or rise
            response_data = e.error_response()
        event.set()
        return response_data

    async def run_job(self):
        exit(1)
