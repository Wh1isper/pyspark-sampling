import pandas
import pytest

from functools import reduce
import os
from sparksampling.error import BAD_PARAM_ERROR, EXHAUSTED_ERROR, PROCESS_ERROR
from sparksampling.proto.sampling_service_pb2 import (
    SamplingRequest,
    RANDOM_SAMPLING_METHOD,
    SIMPLE_SAMPLING_METHOD,
    STRATIFIED_SAMPLING_METHOD,
    UNKNOWN_METHOD,
    FILE_FORMAT_CSV,
    UNKNOWN_FORMAT,
    SamplingConf,
    FileFormatConf, CancelRequest
)
from utilities import JobThread

dir_prefix = os.path.abspath(os.path.dirname(__file__))
input_path = os.path.join(dir_prefix, '../data/10w_x_10.csv')


def setup_module():
    import shutil

    shutil.rmtree(os.path.join(dir_prefix, './output/'), ignore_errors=True)
    shutil.rmtree(os.path.join(dir_prefix, '../data/sampled'), ignore_errors=True)


def test_random_sampling(grpc_stub):
    sampling_col = ['# id', 'X_0', 'X_1', 'X_2', 'y']
    sampling_conf = SamplingConf(
        fraction='0.2',
        with_replacement=False,
        sampling_col=sampling_col
    )
    format_conf = FileFormatConf(
        with_header=True,
        sep=','
    )

    output_path = os.path.join(dir_prefix, './output/test_random_sampling')
    request = SamplingRequest(
        sampling_method=RANDOM_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=input_path,
        output_path=output_path,
        job_id='test_random_sampling'
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == 0
    assert response.data.sampled_path.endswith('.csv')
    if os.path.exists(response.data.sampled_path):
        assert sorted(list(pandas.read_csv(response.data.sampled_path).columns)) == sorted(list(sampling_col))


def test_generate_path(grpc_stub):
    sampling_conf = SamplingConf(
        fraction='0.2',
        with_replacement=False,
    )
    format_conf = FileFormatConf(
        with_header=True,
        sep=','
    )

    request = SamplingRequest(
        sampling_method=RANDOM_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=input_path,
        job_id='test_random_sampling'
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == 0
    assert response.data.sampled_path.endswith('.csv')


def test_simple_sampling(grpc_stub):
    sampling_col = ['# id', 'X_0', 'X_1', 'X_2', 'y']
    sampling_conf = SamplingConf(
        count=1000,
        sampling_col=sampling_col
    )
    format_conf = FileFormatConf(
        with_header=True,
        sep=','
    )
    output_path = os.path.join(dir_prefix, './output/test_simple_sampling')

    request = SamplingRequest(
        sampling_method=SIMPLE_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=input_path,
        output_path=output_path,
        job_id='test_simple_sampling'
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == 0
    assert response.data.sampled_path.endswith('.csv')

    import pandas as pd
    df = pd.read_csv(response.data.sampled_path, sep=',')
    assert df.shape[0] == 1000
    if os.path.exists(response.data.sampled_path):
        assert sorted(list(df.columns)) == sorted(list(sampling_col))

    # bad count in simple sampling
    sampling_conf = SamplingConf(
        count=0,
    )
    request = SamplingRequest(
        sampling_method=SIMPLE_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=input_path,
        output_path=output_path,
        job_id='test_simple_sampling'
    )
    response = grpc_stub.SamplingJob(request)
    assert response.code == BAD_PARAM_ERROR


def test_stratified_sampling(grpc_stub):
    sampling_col = ['# id', 'X_0', 'X_1', 'X_2', 'y']
    fraction = {
        '0': 0.3,
        '1': 0.7
    }
    import json

    sampling_conf = SamplingConf(
        fraction=json.dumps(fraction),
        with_replacement=True,
        stratified_key='y',
        sampling_col=sampling_col
    )
    format_conf = FileFormatConf(
        with_header=True,
        sep=','
    )
    output_path = os.path.join(dir_prefix, './output/test_stratified_sampling')

    request = SamplingRequest(
        sampling_method=STRATIFIED_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=input_path,
        output_path=output_path,
        job_id='test_stratified_sampling'
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == 0
    assert response.data.sampled_path.endswith('.csv')

    import pandas as pd
    df = pd.read_csv(response.data.sampled_path, sep=',')
    assert df[df['y'] == 1].shape[0] > (df[df['y'] == 0].shape[0]) * 2
    if os.path.exists(response.data.sampled_path):
        assert sorted(list(df.columns)) == sorted(list(sampling_col))


def test_simply_stratified_sampling(grpc_stub):
    fraction = '0.5'

    sampling_conf = SamplingConf(
        fraction=fraction,
        with_replacement=True,
        stratified_key='y'
    )
    format_conf = FileFormatConf(
        with_header=True,
        sep=','
    )
    output_path = os.path.join(dir_prefix, './output/test_simply_stratified_sampling')

    request = SamplingRequest(
        sampling_method=STRATIFIED_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=input_path,
        output_path=output_path,
        job_id='test_simply_stratified_sampling'
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == 0
    assert response.data.sampled_path.endswith('.csv')

    import pandas as pd
    df = pd.read_csv(response.data.sampled_path, sep=',', index_col='# id')
    assert df[df['y'] == 1].shape[0] - (df[df['y'] == 0].shape[0]) < 500
    assert 24000 < df[df['y'] == 1].shape[0] < 26000
    assert 24000 < df[df['y'] == 0].shape[0] < 26000


def test_unknown_sampling_failure(grpc_stub):
    sampling_conf = SamplingConf()
    format_conf = FileFormatConf()
    output_path = os.path.join(dir_prefix, './output/test_unknown_sampling_failure')

    request = SamplingRequest(
        sampling_method=UNKNOWN_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=input_path,
        output_path=output_path,
        job_id='test_unknown_sampling_failure'
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == BAD_PARAM_ERROR


def test_unknown_type_failure(grpc_stub):
    sampling_conf = SamplingConf()
    format_conf = FileFormatConf()
    output_path = os.path.join(dir_prefix, './output/test_unknown_type_failure')

    request = SamplingRequest(
        sampling_method=RANDOM_SAMPLING_METHOD,
        file_format=UNKNOWN_FORMAT,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=input_path,
        output_path=output_path,
        job_id='test_unknown_type_failure'
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == BAD_PARAM_ERROR


def test_cancel(grpc_stub):
    request = CancelRequest(
        job_id='test_cancel'
    )
    response = grpc_stub.CancelJob(request)
    return response.code == 0


def example_test_cancel(grpc_stub):
    # 本来想多线程测试，但是pytest似乎只能线性执行，暂时作为示例

    sampling_conf = SamplingConf(
        fraction='0.2',
        with_replacement=False,
    )
    format_conf = FileFormatConf(
        with_header=True,
        sep=','
    )
    output_path = os.path.join(dir_prefix, './output/test_random_sampling')

    def cancel_job():
        request = CancelRequest(
            job_id='test_cancel'
        )
        response = grpc_stub.CancelJob(request)
        return response.code == 0

    def start_job():
        request = SamplingRequest(
            sampling_method=RANDOM_SAMPLING_METHOD,
            file_format=FILE_FORMAT_CSV,
            sampling_conf=sampling_conf,
            format_conf=format_conf,
            input_path=input_path,
            output_path=output_path,
            job_id=f'test_cancel'
        )
        print(f"test_cancel: request for test_cancel")
        response = grpc_stub.SamplingJob(request)
        print(f"test_cancel: finish processing for test_cancel, response.code: {response.code}")
        return response.code == PROCESS_ERROR

    threads = []
    threads.append(JobThread(target=start_job))
    threads.append(JobThread(target=cancel_job))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    results = [t.get_result() for t in threads]
    assert reduce(lambda x, y: x and y, results)


if __name__ == '__main__':
    pytest.main()
