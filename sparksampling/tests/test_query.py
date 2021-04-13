from sparksampling.app import query_app
from sparksampling.tests.base_test_module import BaseTestModule


class BaseTestQueryModule(BaseTestModule):
    def get_app(self):
        return query_app()


class TestSamplingJob(BaseTestQueryModule):
    test_url = '/v1/query/sampling/job/(.*)'

    def test_query(self):
        response = self._post_data_from_file('query.json')
        self._check_code(response, 0, 'Sampling Job Test')


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


class TestEvaluationList(BaseTestQueryModule):
    test_url = '/v1/query/evaluation/list/(.*)'

    def test_query(self):
        response = self._post_data_from_file('list.json')
        self._check_code(response, 0, 'Evaluation List Test')
