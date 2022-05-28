# import for support grpc

from functools import reduce
import pytest
import os
from sparksampling.error import EXHAUSTED_ERROR, PROCESS_ERROR
from sparksampling.proto.sampling_service_pb2 import (
    SamplingRequest,
    RANDOM_SAMPLING_METHOD,
    FILE_FORMAT_CSV,
    SamplingConf,
    FileFormatConf,
    CancelRequest,
)
from utilities import JobThread


@pytest.fixture(scope='module')
def grpc_add_to_server():
    from sparksampling.proto.sampling_service_pb2_grpc import add_SparkSamplingServiceServicer_to_server
    from sparksampling.service import GRPCService

    GRPCService.guarantee_worker = 0
    return add_SparkSamplingServiceServicer_to_server


def test_worker_limit(grpc_stub):
    # 本来想多线程测试，但是pytest似乎只能线性执行，所以上面把guarantee_worker设置为0，一定会触发EXHAUSTED_ERROR
    sampling_conf = SamplingConf(
        fraction='0.2',
        with_replacement=False,
    )
    format_conf = FileFormatConf(
        with_header=True,
        sep=','
    )
    dir_prefix = os.path.abspath(os.path.dirname(__file__))
    input_path = os.path.join(dir_prefix, '../data/10w_x_10.csv')
    output_path = os.path.join(dir_prefix, './output/test_random_sampling')

    def start_job(job_id):
        request = SamplingRequest(
            sampling_method=RANDOM_SAMPLING_METHOD,
            file_format=FILE_FORMAT_CSV,
            sampling_conf=sampling_conf,
            format_conf=format_conf,
            input_path=input_path,
            output_path=output_path,
            job_id=f'test_worker_limit-{job_id}'
        )
        print(f"test_worker_limit: request for job-{job_id}")
        response = grpc_stub.SamplingJob(request)
        print(f"test_worker_limit: finish processing for job-{job_id}, response.code: {response.code}")
        return response.code == EXHAUSTED_ERROR

    threads = []
    for i in range(5):
        t = JobThread(target=start_job, kwargs={'job_id': i})
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    results = [t.get_result() for t in threads]
    assert reduce(lambda x, y: x or y, results)


if __name__ == '__main__':
    pytest.main()
