import os
from functools import reduce

import pandas
import pytest
from utilities import JobThread

from sparksampling.error import BAD_PARAM_ERROR, PROCESS_ERROR
from sparksampling.proto.sampling_service_pb2 import (
    CLUSTER_SAMPLING_METHOD,
    FILE_FORMAT_CSV,
    RANDOM_SAMPLING_METHOD,
    SIMPLE_SAMPLING_METHOD,
    STRATIFIED_SAMPLING_METHOD,
    UNKNOWN_FORMAT,
    UNKNOWN_METHOD,
    CancelRequest,
    FileFormatConf,
    SamplingConf,
    SamplingRequest,
)

dir_prefix = os.path.abspath(os.path.dirname(__file__))
input_path = os.path.join(dir_prefix, "../data/10w_x_10.csv")


def test_random_sampling_fraction(grpc_stub):
    sampling_col = ["# id", "X_0", "X_1", "X_2", "y"]
    sampling_conf = SamplingConf(fraction="0.2", with_replacement=False, sampling_col=sampling_col)
    format_conf = FileFormatConf(with_header=True, sep=",")

    output_path = os.path.join(dir_prefix, "./output/test_random_sampling_fraction")
    request = SamplingRequest(
        sampling_method=RANDOM_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=input_path,
        output_path=output_path,
        job_id="test_random_sampling",
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == 0
    assert response.data.sampled_path.endswith(".csv")
    if os.path.exists(response.data.sampled_path):
        assert sorted(list(pandas.read_csv(response.data.sampled_path).columns)) == sorted(
            list(sampling_col)
        )


def test_random_sampling_count(grpc_stub):
    sampling_col = ["# id", "X_0", "X_1", "X_2", "y"]
    sampling_conf = SamplingConf(count=200, with_replacement=False, sampling_col=sampling_col)
    format_conf = FileFormatConf(with_header=True, sep=",")

    output_path = os.path.join(dir_prefix, "./output/test_random_sampling_count")
    request = SamplingRequest(
        sampling_method=RANDOM_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=input_path,
        output_path=output_path,
        job_id="test_random_sampling",
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == 0
    assert response.data.sampled_path.endswith(".csv")
    if os.path.exists(response.data.sampled_path):
        assert sorted(list(pandas.read_csv(response.data.sampled_path).columns)) == sorted(
            list(sampling_col)
        )


def test_cluster_sampling_num(grpc_stub):
    iris_path = os.path.join(dir_prefix, "../data/iris.csv")
    group_num = 1
    group_by = "Species"

    sampling_conf = SamplingConf(
        group_num=group_num,
        group_by=group_by,
        seed=1,
    )
    format_conf = FileFormatConf(with_header=True, sep=",")

    output_path = os.path.join(dir_prefix, "./output/iris_num_group_sampling")
    request = SamplingRequest(
        sampling_method=CLUSTER_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=iris_path,
        output_path=output_path,
        job_id="test_cluster_sampling_n",
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == 0
    assert response.data.sampled_path.endswith(".csv")
    import pandas as pd

    pdf = pd.read_csv(response.data.sampled_path)
    assert int(pdf[[group_by]].drop_duplicates().count()) == group_num


def test_cluster_sampling_fraction(grpc_stub):
    iris_path = os.path.join(dir_prefix, "../data/iris.csv")

    sampling_conf = SamplingConf(
        fraction="0.5",
        group_by="Species",
        seed=1,
    )
    format_conf = FileFormatConf(with_header=True, sep=",")

    output_path = os.path.join(dir_prefix, "./output/iris_fraction_group_sampling")
    request = SamplingRequest(
        sampling_method=CLUSTER_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=iris_path,
        output_path=output_path,
        job_id="test_cluster_sampling_f",
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == 0
    assert response.data.sampled_path.endswith(".csv")


def test_cluster_sampling_fraction_num(grpc_stub):
    iris_path = os.path.join(dir_prefix, "../data/iris.csv")
    group_num = 2
    group_by = "Species"

    sampling_conf = SamplingConf(
        fraction="0.5",
        group_by=group_by,
        group_num=group_num,
        seed=1,
    )
    format_conf = FileFormatConf(with_header=True, sep=",")

    output_path = os.path.join(dir_prefix, "./output/iris_fraction_num_group_sampling")
    request = SamplingRequest(
        sampling_method=CLUSTER_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=iris_path,
        output_path=output_path,
        job_id="test_cluster_sampling_fn",
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == 0
    assert response.data.sampled_path.endswith(".csv")
    import pandas as pd

    pdf = pd.read_csv(response.data.sampled_path)
    assert int(pdf[[group_by]].drop_duplicates().count()) <= group_num


def test_cluster_sampling_empty(grpc_stub):
    iris_path = os.path.join(dir_prefix, "../data/empty.csv")
    group_num = 1
    group_by = "user"

    sampling_conf = SamplingConf(
        group_num=group_num,
        group_by=group_by,
        seed=1,
    )
    format_conf = FileFormatConf(with_header=True, sep=",")

    output_path = os.path.join(dir_prefix, "./output/empty")
    request = SamplingRequest(
        sampling_method=CLUSTER_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=iris_path,
        output_path=output_path,
        job_id="test_cluster_sampling_empty",
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == 0
    assert response.data.sampled_path.endswith(".csv")
    import pandas as pd

    pdf = pd.read_csv(response.data.sampled_path)
    assert int(pdf[[group_by]].drop_duplicates().count()) == 0
    for hook_msg in response.data.hook_msg:
        if hook_msg.hook_name == "EmptyDetectHook":
            for meta in hook_msg.meta:
                if meta.key == "is_not_empty":
                    assert meta.value == "False"


def test_generate_path(grpc_stub):
    sampling_conf = SamplingConf(
        fraction="0.2",
        with_replacement=False,
    )
    format_conf = FileFormatConf(with_header=True, sep=",")

    request = SamplingRequest(
        sampling_method=RANDOM_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=input_path,
        job_id="test_random_sampling",
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == 0
    assert response.data.sampled_path.endswith(".csv")


def test_simple_sampling(grpc_stub):
    sampling_col = ["# id", "X_0", "X_1", "X_2", "y"]
    sampling_conf = SamplingConf(count=1000, sampling_col=sampling_col)
    format_conf = FileFormatConf(with_header=True, sep=",")
    output_path = os.path.join(dir_prefix, "./output/test_simple_sampling")

    request = SamplingRequest(
        sampling_method=SIMPLE_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=input_path,
        output_path=output_path,
        job_id="test_simple_sampling",
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == 0
    assert response.data.sampled_path.endswith(".csv")

    import pandas as pd

    df = pd.read_csv(response.data.sampled_path, sep=",")
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
        job_id="test_simple_sampling",
    )
    response = grpc_stub.SamplingJob(request)
    assert response.code == BAD_PARAM_ERROR


def test_stratified_sampling(grpc_stub):
    sampling_col = ["# id", "X_0", "X_1", "X_2", "y"]
    fraction = {"0": 0.3, "1": 0.7}
    import json

    sampling_conf = SamplingConf(
        fraction=json.dumps(fraction),
        with_replacement=True,
        stratified_key="y",
        sampling_col=sampling_col,
    )
    format_conf = FileFormatConf(with_header=True, sep=",")
    output_path = os.path.join(dir_prefix, "./output/test_stratified_sampling")

    request = SamplingRequest(
        sampling_method=STRATIFIED_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=input_path,
        output_path=output_path,
        job_id="test_stratified_sampling",
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == 0
    assert response.data.sampled_path.endswith(".csv")

    import pandas as pd

    df = pd.read_csv(response.data.sampled_path, sep=",")
    assert df[df["y"] == 1].shape[0] > (df[df["y"] == 0].shape[0]) * 2
    if os.path.exists(response.data.sampled_path):
        assert sorted(list(df.columns)) == sorted(list(sampling_col))


def test_simply_stratified_sampling(grpc_stub):
    fraction = "0.5"

    sampling_conf = SamplingConf(fraction=fraction, with_replacement=True, stratified_key="y")
    format_conf = FileFormatConf(with_header=True, sep=",")
    output_path = os.path.join(dir_prefix, "./output/test_simply_stratified_sampling")

    request = SamplingRequest(
        sampling_method=STRATIFIED_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=input_path,
        output_path=output_path,
        job_id="test_simply_stratified_sampling",
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == 0
    assert response.data.sampled_path.endswith(".csv")

    import pandas as pd

    df = pd.read_csv(response.data.sampled_path, sep=",", index_col="# id")
    assert df[df["y"] == 1].shape[0] - (df[df["y"] == 0].shape[0]) < 500
    assert 24000 < df[df["y"] == 1].shape[0] < 26000
    assert 24000 < df[df["y"] == 0].shape[0] < 26000


def test_unknown_sampling_failure(grpc_stub):
    sampling_conf = SamplingConf()
    format_conf = FileFormatConf()
    output_path = os.path.join(dir_prefix, "./output/test_unknown_sampling_failure")

    request = SamplingRequest(
        sampling_method=UNKNOWN_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=input_path,
        output_path=output_path,
        job_id="test_unknown_sampling_failure",
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == BAD_PARAM_ERROR


def test_unknown_type_failure(grpc_stub):
    sampling_conf = SamplingConf()
    format_conf = FileFormatConf()
    output_path = os.path.join(dir_prefix, "./output/test_unknown_type_failure")

    request = SamplingRequest(
        sampling_method=RANDOM_SAMPLING_METHOD,
        file_format=UNKNOWN_FORMAT,
        sampling_conf=sampling_conf,
        format_conf=format_conf,
        input_path=input_path,
        output_path=output_path,
        job_id="test_unknown_type_failure",
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == BAD_PARAM_ERROR


def test_cancel(grpc_stub):
    request = CancelRequest(job_id="test_cancel")
    response = grpc_stub.CancelJob(request)
    return response.code == 0


def example_test_cancel(grpc_stub):
    sampling_conf = SamplingConf(
        fraction="0.2",
        with_replacement=False,
    )
    format_conf = FileFormatConf(with_header=True, sep=",")
    output_path = os.path.join(dir_prefix, "./output/test_random_sampling")

    def cancel_job():
        request = CancelRequest(job_id="test_cancel")
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
            job_id=f"test_cancel",
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


if __name__ == "__main__":
    pytest.main()
