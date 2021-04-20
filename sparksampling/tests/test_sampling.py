from sparksampling.app import sampling_app
from sparksampling.tests.base_test_module import BaseTestModule


class BaseTestSampleModule(BaseTestModule):
    def get_app(self):
        return sampling_app()


class TestSimpleSampling(BaseTestSampleModule):
    test_url = '/v1/sampling/simplejob/'

    def test_random_sampling(self):
        response = self._post_data_from_file('simple-random-sampling.json')
        self._check_code(response, 0, 'Sample Random Sampling Job Submit Test')

    def test_random_sampling_txt(self):
        response = self._post_data_from_file('simple-random-sampling-txt.json')
        self._check_code(response, 0, 'Sample Random Sampling Job Submit Test')

    def test_stratified_sampling(self):
        response = self._post_data_from_file('stratified-sampling.json')
        self._check_code(response, 0, 'Stratified Sampling Job Submit Test')


class TestMLSampling(BaseTestSampleModule):
    test_url = '/v1/sampling/mljob/'

    def test_2_class_upper_smote(self):
        response = self._post_data_from_file('smote-sampling.json')

        self._check_code(response, 0, 'SMOTE Job Submit Test')
