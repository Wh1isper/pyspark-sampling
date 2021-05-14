from urllib.parse import urljoin

from sparksampling.var import *
import json
import requests
import pandas as pd
import logging

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

import ast


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

    def to_pandas(self):
        if not self.is_response_ok:
            return f"Request Error...Check: {self.to_dict()}"
        if type(self.data) is list:
            return pd.DataFrame.from_records(self.data, index='summary')
        elif self.data.get('result'):
            data = ast.literal_eval(self.data['result'])
            return pd.DataFrame.from_records(data)
        else:
            logging.info("No Data to Translate to pandas.")
            logging.info(self.to_dict())

    @property
    def job_id(self):
        return self.data.get('job_id')

    @property
    def sampled_path(self):
        return self.data.get('simpled_file_path')

    @property
    def is_response_ok(self):
        return self.code == 0

    @property
    def is_job_ok(self):
        return self.data.get('job_status') == CODE_TO_JOB_STATUS[JOB_STATUS_SUCCEED]

    def __str__(self):
        return self.to_dict()

    def __getitem__(self, item):
        return self.__dict__[item]


class Submitter(object):
    def __init__(self, ip='localhost', port=8000, protocol='http'):
        self.ip = ip
        self.port = port
        self.prefix = f'{protocol}://{self.ip}:{self.port}/'

        self.__simple_job_url = urljoin(self.prefix, '/v1/sampling/simplejob/')
        self.__ml_job_url = urljoin(self.prefix, '/v1/sampling/mljob/')
        self.__evaluation_job_url = urljoin(self.prefix, '/v1/evaluation/job/')
        self.__statistics_job_url = urljoin(self.prefix, '/v1/evaluation/statistics/')
        self.__query_simple_job_detail_url = urljoin(self.prefix, '/v1/query/sampling/job/')
        self.__query_simple_job_list_url = urljoin(self.prefix, '/v1/query/sampling/list/')
        self.__query_evaluation_job_detail_url = urljoin(self.prefix, '/v1/query/evaluation/job/')
        self.__query_evaluation_job_list_url = urljoin(self.prefix, '/v1/query/evaluation/list/')

    def submit_sampling_simplejob(self, path, method=SIMPLE_RANDOM_SAMPLING_METHOD, file_type=FILE_TYPE_TEXT,
                                  with_header=None, seed=None,
                                  fraction: str or dict = None,
                                  with_replacement: bool = None, key=None):
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
        return DSResponse(**self._post_dict_data(self.__simple_job_url, config_map))

    def submit_sampling_mljob(self, path, method=SPARK_SMOTE_SAMPLING_METHOD, file_type=FILE_TYPE_TEXT,
                              with_header=None, key=None, drop_list=None, k_neighbors=None, n_neighbors=None,
                              conf=None):
        if conf is None:
            conf = {}
        if drop_list is None:
            drop_list = []
        algo_conf = {
            "n_neighbors": n_neighbors,
            "k_neighbors": k_neighbors,
            "key": key,
            "drop_list": drop_list,
        }.update(conf)
        config_map = {
            'path': path,
            'method': method,
            'type': file_type,
            'with_header': with_header,
            'conf': algo_conf,
        }
        return DSResponse(**self._post_dict_data(self.__ml_job_url, config_map))

    def submit_evaluation_job(self, path=None, source_path=None, compare_job_id=None, method=EVALUATION_COMPARE_METHOD,
                              file_type=FILE_TYPE_TEXT,
                              with_header=None,
                              **kwargs):
        config_map = {
            'path': path,
            'source_path': source_path,
            'method': method,
            'type': file_type,
            'with_header': with_header,
            'compare_job_id': compare_job_id
        }
        config_map.update(kwargs)
        return DSResponse(**self._post_dict_data(self.__evaluation_job_url, config_map))

    def get_sampling_job_details(self, job_id):
        config_map = {
            'job_id': job_id,
        }
        return DSResponse(**self._post_dict_data(self.__query_simple_job_detail_url, config_map))

    def get_sampling_job_list(self, offset=None, limit=None):
        config_map = {
            'offset': offset,
            'limit': limit,
        }
        return DSResponse(**self._post_dict_data(self.__query_simple_job_list_url, config_map))

    def get_evaluation_job_details(self, job_id):
        config_map = {
            'job_id': job_id,
        }
        return DSResponse(**self._post_dict_data(self.__query_evaluation_job_detail_url, config_map))

    def get_evaluation_job_list(self, offset=None, limit=None):
        config_map = {
            'offset': offset,
            'limit': limit,
        }
        return DSResponse(**self._post_dict_data(self.__query_evaluation_job_list_url, config_map))

    def get_statistics(self, path=None,
                       job_id=None,
                       file_type=FILE_TYPE_TEXT,
                       method=STATISTICS_BASIC_METHOD,
                       with_header=None,
                       from_sampling=False):
        if from_sampling and not job_id:
            return "Error: From sampling should set a job_id"
        config_map = {
            'path': path,
            'job_id': job_id,
            'type': file_type,
            'method': method,
            'with_header': with_header,
            'from_sampling': from_sampling
        }
        return DSResponse(**self._post_dict_data(self.__statistics_job_url, config_map))

    def _post_dict_data(self, url, data: dict):
        extract_none_in_dict(data)
        request_body = json.dumps(data)
        logging.info(f"request: {url} with data {data}")
        request = requests.post(url, data=request_body)
        return request.json() if request.status_code is requests.codes.ok else {
            'code': request.status_code,
            'msg': 'HTTP ERROR',
            'data': {},
        }
