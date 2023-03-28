import json
import os
import pathlib
import subprocess

import pytest
from sparksampling_client.proto.sampling_service_pb2 import (
    FILE_FORMAT_CSV,
    RANDOM_SAMPLING_METHOD,
)


@pytest.fixture(scope="module")
def request_dict():
    sampling_col = ["# id", "X_0", "X_1", "X_2", "y"]
    dir_prefix = os.path.abspath(os.path.dirname(__file__))
    input_path = os.path.join(dir_prefix, "../../data/10w_x_10.csv")
    output_path = os.path.join(dir_prefix, "./output/test_random_sampling_fraction")

    return dict(
        sampling_method=RANDOM_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=dict(fraction="0.2", with_replacement=False, sampling_col=sampling_col),
        format_conf=dict(with_header=True, sep=","),
        input_path=input_path,
        output_path=output_path,
        job_id="test_random_sampling",
    )


@pytest.fixture
def json_request_file(tmpdir, request_dict):
    json_file = pathlib.Path(tmpdir / "reqeust.json")
    with json_file.open("w") as f:
        json.dump(request_dict, f)
    return json_file


def test_apply(json_request_file):
    subprocess.run(["ssc", "apply", json_request_file], check=True)


if __name__ == "__main__":
    pytest.main()
