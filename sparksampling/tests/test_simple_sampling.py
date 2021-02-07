from sparksampling.app import sampling_app
from sparksampling.tests.base_test_module import BaseTestModule


class TestSimpleSamplingModule(BaseTestModule):
    test_url = '/v1/sampling/simplejob/'

    def get_app(self):
        return sampling_app()

    def test_random_sampling(self):
        response = self._post_data_from_file('simple-random-sampling.json')
        self._check_code(response, 0, 'Sample Random Sampling Test')

    def test_stratified_sampling(self):
        response = self._post_data_from_file('stratified-sampling.json')
        self._check_code(response, 0, 'Stratified Sampling Test')
