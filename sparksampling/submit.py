from urllib.parse import urljoin

from sparksampling.utilities.var import FILE_TYPE_TEXT, SIMPLE_RANDOM_SAMPLING_METHOD, STATISTICS_BASIC_METHOD, \
    SMOTE_SAMPLING_METHOD
import json
import requests


def extract_none_in_dict(d: dict):
    items = list(d.items())
    for k, v in items:
        if type(v) is dict:
            extract_none_in_dict(v)
        if v is None:
            d.pop(k)


class DSResponse(object):
    def __init__(self, code, msg, data):
        self.code = code
        self.msg = msg
        self.data = data

    def to_dict(self):
        return {
            'code': self.code,
            'msg': self.msg,
            'data': self.data,
        }

    def __str__(self):
        return self.to_dict()


class Submitter(object):
    def __init__(self, ip='localhost', query_port=8000, sampling_port=8000, evaluation_port=8000, protocol='http'):
        self.ip = ip
        self.query_port = query_port
        self.sampling_port = sampling_port
        self.evaluation_port = evaluation_port
        self.query_prefix = f'{protocol}://{self.ip}:{self.query_port}/'
        self.sampling_prefix = f'{protocol}://{self.ip}:{self.sampling_port}/'
        self.evaluation_prefix = f'{protocol}://{self.ip}:{self.evaluation_port}/'

    def submit_sampling_simplejob(self, path, method=SIMPLE_RANDOM_SAMPLING_METHOD, file_type=FILE_TYPE_TEXT,
                                  with_header=None, seed=None,
                                  fraction: str or dict = None,
                                  with_replacement: bool = None, key=None):
        url = urljoin(self.sampling_prefix, '/v1/sampling/simplejob/')
        config_map = {
            'path': path,
            'method': method,
            'type': file_type,
            'with_header': with_header,
            'conf': {
                'seed': seed,
                'fraction': fraction,
                'with_replacement': with_replacement,
                'key': key
            }
        }
        return DSResponse(**self._post_dict_data(url, config_map))

    def submit_sampling_mljob(self, path, method=SMOTE_SAMPLING_METHOD, file_type=FILE_TYPE_TEXT, with_header=None,
                              **kwargs):
        url = urljoin(self.sampling_prefix, '/v1/sampling/mljob')
        config_map = {
            'path': path,
            'method': method,
            'type': file_type,
            'with_header': with_header,
            'conf': kwargs
        }
        return DSResponse(**self._post_dict_data(url, config_map))

    def get_sampling_job_details(self, job_id):
        url = urljoin(self.sampling_prefix, '/v1/sampling/query/job/')
        config_map = {
            'job_id': job_id,
        }
        return self._post_dict_data(url, config_map)

    def get_sampling_job_list(self, offset=None, limit=None):
        url = urljoin(self.sampling_prefix, '/v1/sampling/query/list/')
        config_map = {
            'offset': offset,
            'limit': limit,
        }
        return DSResponse(**self._post_dict_data(url, config_map))

    def get_statistics(self, path=None,
                       job_id=None,
                       file_type=FILE_TYPE_TEXT,
                       method=STATISTICS_BASIC_METHOD,
                       with_header=None,
                       from_sampling=False):
        if from_sampling and not job_id:
            return "From sampling should set a job_id"
        url = urljoin(self.sampling_prefix, '/v1/evaluation/statistics/')
        config_map = {
            'path': path,
            'job_id': job_id,
            'type': file_type,
            'method': method,
            'with_header': with_header,
            'from_sampling': from_sampling
        }
        return DSResponse(**self._post_dict_data(url, config_map))

    def _post_dict_data(self, url, data: dict):
        extract_none_in_dict(data)
        request_body = json.dumps(data)
        print(f"request: {url}")
        request = requests.post(url, data=request_body)
        return request.json() if request.status_code is requests.codes.ok else {
            'code': request.status_code,
            'msg': 'HTTP ERROR',
            'data': {},
        }