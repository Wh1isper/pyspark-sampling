import logging
import time
from typing import List

import json
from sparksampling import Submitter
from sparksampling.submit import DSResponse
from sparksampling.var import *

submitter = Submitter()
drop_list = ['# id', 'y']

SAMPLING_JOB_TYPE = 'sampling'
EVALUATION_JOB_TYPE = 'evaluation'
JOB_TYPE = [SAMPLING_JOB_TYPE, EVALUATION_JOB_TYPE]


def get_path(job_type: str):
    if job_type not in JOB_TYPE:
        return ''
    else:
        return "timming_{}_job_list.json".format(job_type)


def query_job_info(job_list: List[int], job_type: str) -> List[DSResponse]:
    def request_until_done(func, job_id):
        job_info = func(job_id=job_id)
        while not job_info.is_job_ok:
            time.sleep(2)
            logging.info(f"Waiting job {job_info.job_id}")
            job_info = func(job_id)
        return job_info

    if job_type == SAMPLING_JOB_TYPE:
        func = submitter.get_sampling_job_details
    elif job_type == EVALUATION_JOB_TYPE:
        func = submitter.get_evaluation_job_details
    else:
        return []
    job_info_list = [request_until_done(func, job_id) for job_id in job_list]
    return job_info_list


def get_job_id_list(job_list: List[DSResponse]):
    return [submit_response.job_id for submit_response in job_list]


def save_job_id(job_list: List[DSResponse], job_type: str) -> str:
    path = get_path(job_type)
    if path:
        job_id_list = get_job_id_list(job_list)
        with open(path, 'w') as f:
            json.dump(job_id_list, f)
    return path


def generate_sampling_job(dataset_uri, fraction=0.1, with_header=True, method=SIMPLE_RANDOM_SAMPLING_METHOD, num=10,
                          save=True) -> List[DSResponse]:
    def submit_sampling_job_wait_finish(dataset_uri, fraction=0.1, with_header=True,
                                        method=SIMPLE_RANDOM_SAMPLING_METHOD, ):
        response = submitter.submit_sampling_simplejob(dataset_uri,
                                                       method=method,
                                                       file_type=FILE_TYPE_CSV,
                                                       fraction=fraction,
                                                       with_header=with_header)

        job_details = submitter.get_sampling_job_details(response.job_id)
        while not job_details.is_job_ok:
            time.sleep(2)
            job_details = submitter.get_sampling_job_details(response.job_id)
        return response

    job_list = [submit_sampling_job_wait_finish(dataset_uri,
                                                method=method,
                                                fraction=fraction,
                                                with_header=with_header) for _ in range(num)]
    if save:
        save_job_id(job_list, job_type=SAMPLING_JOB_TYPE)
    return job_list


def generate_evaluation_job(compare_job_id_list: List[int], save=True) -> List[DSResponse]:
    def submit_evaluation_job_wait_finish(cmp_job_id):
        response = submitter.submit_evaluation_job(compare_job_id=cmp_job_id, file_type=FILE_TYPE_CSV)
        job_details = submitter.get_evaluation_job_details(response.job_id)
        while not job_details.is_job_ok:
            time.sleep(2)
            job_details = submitter.get_evaluation_job_details(response.job_id)
        return response

    job_list = [submit_evaluation_job_wait_finish(cmp_job_id) for cmp_job_id in
                compare_job_id_list]
    if save:
        save_job_id(job_list, job_type=EVALUATION_JOB_TYPE)
    return job_list


def load_from_json():
    def load_job_info(job_type: str, file_path=None) -> List[int]:
        path = get_path(job_type) if not file_path else file_path
        if path:
            with open(path, 'r') as f:
                job_id_list = json.load(f)
            return job_id_list

    job_list = load_job_info(SAMPLING_JOB_TYPE)
    eva_job_list = load_job_info(EVALUATION_JOB_TYPE)
    sampling_job_details = query_job_info(job_list, SAMPLING_JOB_TYPE)
    eva_job_details = query_job_info(eva_job_list, EVALUATION_JOB_TYPE)
    return sampling_job_details, eva_job_details


def new_exp(dataset_uri, num):
    response_list = generate_sampling_job(dataset_uri, num=num)
    job_list = get_job_id_list(response_list)
    sampling_job_details = query_job_info(job_list, SAMPLING_JOB_TYPE)

    eva_response_job_list = generate_evaluation_job(job_list)
    eva_job_list = get_job_id_list(eva_response_job_list)
    eva_job_details = query_job_info(eva_job_list, EVALUATION_JOB_TYPE)

    return sampling_job_details, eva_job_details


if __name__ == '__main__':
    num = 2
    dataset_uri = 'hdfs://localhost:9000/dataset/ten_million_top1k.csv'
    NEW_EXP = True

    sampling_job_details, eva_job_details = new_exp(dataset_uri, num) if NEW_EXP else load_from_json()
