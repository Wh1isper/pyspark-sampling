import pytest

from sparksampling.proto.sampling_service_pb2 import (
    FILE_FORMAT_CSV,
    RANDOM_SAMPLING_METHOD,
    FileFormatConf,
    SamplingConf,
    SamplingRequest,
)


@pytest.mark.xfail
def test_random_sampling(grpc_stub):
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
        input_path="s3a://datasampling-test/object",
        output_path="s3a://datasampling-test/object-unittest",
        job_id="test_random_sampling",
    )

    response = grpc_stub.SamplingJob(request)
    assert response.code == 0
    assert response.data.sampled_path.endswith(".csv")
