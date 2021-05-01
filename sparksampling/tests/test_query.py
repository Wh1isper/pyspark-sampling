from sparksampling.app import query_app
from sparksampling.tests.base_test_module import BaseTestModule
from sparksampling.handler.processmodule import BaseQueryProcessModule


class BaseTestQueryModule(BaseTestModule):
    def get_app(self):
        return query_app()


class TestSamplingJob(BaseTestQueryModule):
    test_url = '/v1/query/sampling/job/(.*)'

    def test_query(self):
        response = self._post_data_from_file('query.json')
        self._check_code(response, 0, 'Sampling Job Test')

    def test_job_not_exist(self):
        request_data = self.dict_to_json({
            'job_id': 1
        })
        response = self.fetch(self.test_url, method='POST', body=request_data, connect_timeout=0, request_timeout=0)
        response_data = self._get_response_data(response)
        self._check_msg(response_data, BaseQueryProcessModule.MSG_JOB_NOT_FOUND, 'Evaluation Job Not Exist Test')


class TestSamplingList(BaseTestQueryModule):
    test_url = '/v1/query/sampling/list/(.*)'

    def test_query(self):
        response = self._post_data_from_file('list.json')
        self._check_code(response, 0, 'Sampling List Test')


class TestEvaluationJob(BaseTestQueryModule):
    test_url = '/v1/query/evaluation/job/(.*)'

    def test_query(self):
        response = self._post_data_from_file('query.json')
        self._check_code(response, 0, 'Evaluation Job Test')

    def test_job_not_exist(self):
        request_data = self.dict_to_json({
            'job_id': 1
        })
        response = self.fetch(self.test_url, method='POST', body=request_data, connect_timeout=0, request_timeout=0)
        response_data = self._get_response_data(response)
        self._check_msg(response_data, BaseQueryProcessModule.MSG_JOB_NOT_FOUND, 'Evaluation Job Not Exist Test')


class TestEvaluationList(BaseTestQueryModule):
    test_url = '/v1/query/evaluation/list/(.*)'

    def test_query(self):
        response = self._post_data_from_file('list.json')
        self._check_code(response, 0, 'Evaluation List Test')
