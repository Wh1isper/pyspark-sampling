from sparksampling.app import evaluation_app
from sparksampling.tests.base_test_module import BaseTestModule


class BaseTestEvaluationModule(BaseTestModule):
    def get_app(self):
        return evaluation_app()

    def test_json_decode_error_code(self):
        super(BaseTestEvaluationModule, self).test_json_decode_error_code()


class TestBasicStatistics(BaseTestEvaluationModule):
    test_url = '/v1/evaluation/statistics/'

    def test_basic_statistics(self):
        response = self._post_data_from_file('statistics-job.json')
        self._check_code(response, 0, 'Basic Statistics Test By Job')

    def test_basic_statistics_by_path(self):
        response = self._post_data_from_file('statistics-path.json')
        self._check_code(response, 0, 'Basic Statistics Test By Path')


class TestEvaluation(BaseTestEvaluationModule):
    test_url = '/v1/evaluation/job/'

    def test_compare_evaluation(self):
        response = self._post_data_from_file('evaluation-compare.json')
        self._check_code(response, 0, 'Compare Evaluation Test')

    def test_kmeans_evaluation(self):
        response = self._post_data_from_file('evaluation-kmeans.json')
        self._check_code(response, 0, 'Kmeans Evaluation Test')

