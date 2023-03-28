import os
from functools import reduce

import pytest
from utilities import JobThread

from sparksampling.error import EXHAUSTED_ERROR
from sparksampling.proto.sampling_service_pb2 import (
    FILE_FORMAT_CSV,
    RANDOM_SAMPLING_METHOD,
    STRATIFIED_SAMPLING_METHOD,
    FileFormatConf,
    SamplingConf,
    SamplingRequest,
)

dir_prefix = os.path.abspath(os.path.dirname(__file__))
input_path = os.path.join(dir_prefix, "../data/unbalance_500v50_10.csv")


@pytest.fixture
def exhausted_worker():
    from sparksampling.engine_factory import EngineFactory

    EngineFactory.register_all_engine()
    for engine in EngineFactory.engine_cls:
        engine.guarantee_worker = 0
    yield
    for engine in EngineFactory.engine_cls:
        engine.guarantee_worker = 10


def test_worker_limit(grpc_stub, exhausted_worker):
    # 本来想多线程测试，但是pytest似乎只能线性执行，所以上面把guarantee_worker设置为0，一定会触发EXHAUSTED_ERROR
    sampling_conf = SamplingConf(
        fraction="0.2",
        with_replacement=False,
    )
    format_conf = FileFormatConf(with_header=True, sep=",")
    dir_prefix = os.path.abspath(os.path.dirname(__file__))
    input_path = os.path.join(dir_prefix, "../data/10w_x_10.csv")
    output_path = os.path.join(dir_prefix, "./output/test_random_sampling")

    def start_job(job_id):
        request = SamplingRequest(
            sampling_method=RANDOM_SAMPLING_METHOD,
            file_format=FILE_FORMAT_CSV,
            sampling_conf=sampling_conf,
            format_conf=format_conf,
            input_path=input_path,
            output_path=output_path,
            job_id=f"test_worker_limit-{job_id}",
        )
        print(f"test_worker_limit: request for job-{job_id}")
        response = grpc_stub.SamplingJob(request)
        print(
            f"test_worker_limit: finish processing for job-{job_id}, response.code: {response.code}"
        )
        return response.code == EXHAUSTED_ERROR

    threads = []
    for i in range(5):
        t = JobThread(target=start_job, kwargs={"job_id": i})
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    results = [t.get_result() for t in threads]
    assert reduce(lambda x, y: x or y, results)


def test_stratified_sampling_reserve(grpc_stub):
    fraction = "0.5"

    sampling_conf = SamplingConf(
        fraction=fraction,
        with_replacement=True,
        stratified_key="# id",
        ensure_col=True,
    )
    format_conf = FileFormatConf(with_header=True, sep=",")
    output_path = os.path.join(dir_prefix, "./output/test_simply_stratified_sampling_reserve")

    request = SamplingRequest(
        sampling_method=STRATIFIED_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=input_path,
        output_path=output_path,
        job_id="test_simply_stratified_sampling_feature",
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == 0
    assert response.data.sampled_path.endswith(".csv")

    import pandas as pd

    df = pd.read_csv(response.data.sampled_path, sep=",", index_col="# id")
    assert df.shape[0] == 550


if __name__ == "__main__":
    pytest.main()
