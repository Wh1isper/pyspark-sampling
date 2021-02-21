from sparksampling.app import query_app
from sparksampling.tests.base_test_module import BaseTestModule


class BaseTestQueryModule(BaseTestModule):
    def get_app(self):
        return query_app()


class TestSamplingJob(BaseTestQueryModule):
    test_url = '/v1/sampling/query/job/(.*)'

    def test_query(self):
        response = self._post_data_from_file('query.json')
        self._check_code(response, 0, 'Sampling Job Test')


class TestSamplingList(BaseTestQueryModule):
    test_url = '/v1/sampling/query/list/(.*)'

    def test_query(self):
        response = self._post_data_from_file('list.json')
        self._check_code(response, 0, 'Sampling List Test')


class TestEvaluationJob(BaseTestQueryModule):
    test_url = '/v1/evaluation/query/job/(.*)'

    def test_query(self):
        response = self._post_data_from_file('query.json')
        self._check_code(response, 0, 'Evaluation Job Test')


class TestEvaluationList(BaseTestQueryModule):
    test_url = '/v1/evaluation/query/list/(.*)'

    def test_query(self):
        response = self._post_data_from_file('list.json')
        self._check_code(response, 0, 'Evaluation List Test')
