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

    def test_spark_smote(self):
        response = self._post_data_from_file('smote-sampling.json')
        self._check_code(response, 0, 'Spark SMOTE Job Submit Test')

    def test_imb_smote(self):
        response = self._post_data_from_file('imb-smote-sampling.json')
        self._check_code(response, 0, 'Imb SMOTE Job Submit Test')

    def test_spark_enn(self):
        response = self._post_data_from_file('enn-sampling.json')
        self._check_code(response, 0, 'Spark ENN Job Submit Test')

    def test_imb_enn(self):
        response = self._post_data_from_file('imb-enn-sampling.json')
        self._check_code(response, 0, 'Imb ENN Job Submit Test')

    def test_spark_smote_enn(self):
        response = self._post_data_from_file('smote-enn-sampling.json')
        self._check_code(response, 0, 'Spark SMOTE-ENN Job Submit Test')

    def test_imb_smote_enn(self):
        response = self._post_data_from_file('imb-smote-enn-sampling.json')
        self._check_code(response, 0, 'Imb SMOTE-ENN Job Submit Test')
