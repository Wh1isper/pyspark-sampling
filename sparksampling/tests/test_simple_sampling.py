from sparksampling.app import sampling_app
from sparksampling.tests.base_test_module import BaseTestModule


class TestSimpleSamplingModule(BaseTestModule):
    test_url = '/v1/sampling/simplejob/'

    def get_app(self):
        return sampling_app()

    def test_random_sampling(self):
        ...

