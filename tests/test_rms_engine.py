import os

import pytest

from sparksampling.proto.sampling_service_pb2 import (
    FILE_FORMAT_CSV,
    RANDOM_SAMPLING_METHOD,
    FileFormatConf,
    RelationSamplingRequest,
    SamplingConf,
)

dir_prefix = os.path.abspath(os.path.dirname(__file__))
input_path = os.path.join(dir_prefix, "../data/10w_x_10.csv")
input_a = os.path.join(dir_prefix, "../data/relation-a.csv")
input_b = os.path.join(dir_prefix, "../data/relation-b.csv")
input_c = os.path.join(dir_prefix, "../data/relation-c.csv")
SEED = 4


def test_only_sampling(grpc_stub):
    sampling_stage = RelationSamplingRequest.SamplingStage(
        input_path=input_a,
        output_path=os.path.join(dir_prefix, "./output/only-sampling/a-sampled"),
        name="sampling-stage",
        sampling_method=RANDOM_SAMPLING_METHOD,
        file_format=FILE_FORMAT_CSV,
        sampling_conf=SamplingConf(
            fraction="0.5",
            with_replacement=True,
            seed=SEED,
        ),
        format_conf=FileFormatConf(with_header=True, sep=","),
    )
    request = RelationSamplingRequest(
        sampling_stages=[sampling_stage],
        job_id="only-sampling-job",
    )

    response = grpc_stub.RelationSamplingJob(request)
    assert response.code == 0


def test_only_relation(grpc_stub):
    relation_b = RelationSamplingRequest.RelationStage(
        input_path=input_b,
        output_path=os.path.join(dir_prefix, "./output/only-relation/b-sampled"),
        name="b",
        relations=[
            RelationSamplingRequest.Relation(
                relation_path=input_a, relation_col="user_id", input_col="user_id"
            )
        ],
    )

    relation_c = RelationSamplingRequest.RelationStage(
        input_path=input_c,
        output_path=os.path.join(dir_prefix, "./output/only-relation/c-sampled"),
        name="c",
        relations=[
            RelationSamplingRequest.Relation(
                relation_name="b", relation_col="session_id", input_col="session_id"
            )
        ],
    )

    request = RelationSamplingRequest(
        sampling_stages=[],
        relation_stages=[relation_b, relation_c],
        job_id="only-relation-job",
        default_sampling_method=RANDOM_SAMPLING_METHOD,
        default_file_format=FILE_FORMAT_CSV,
        default_sampling_conf=SamplingConf(
            fraction="0.5",
            with_replacement=True,
            seed=SEED,
        ),
        default_format_conf=FileFormatConf(with_header=True, sep=","),
    )

    response = grpc_stub.RelationSamplingJob(request)
    assert response.code == 0


def test_part_relation(grpc_stub):
    sampling_a = RelationSamplingRequest.SamplingStage(
        input_path=input_a,
        output_path=os.path.join(dir_prefix, "./output/part-relation/a-sampled"),
        name="a",
    )
    relation_b = RelationSamplingRequest.RelationStage(
        input_path=input_b,
        output_path=os.path.join(dir_prefix, "./output/part-relation/b-sampled"),
        name="b",
        relations=[
            RelationSamplingRequest.Relation(
                relation_path=input_a,
                relation_col="user_id",
            )
        ],
    )

    relation_c = RelationSamplingRequest.RelationStage(
        input_path=input_c,
        output_path=os.path.join(dir_prefix, "./output/part-relation/c-sampled"),
        name="c",
        relations=[
            RelationSamplingRequest.Relation(
                relation_path=input_b,
                relation_col="session_id",
            )
        ],
    )

    request = RelationSamplingRequest(
        sampling_stages=[sampling_a],
        relation_stages=[relation_b, relation_c],
        job_id="part-relation-job",
        default_sampling_method=RANDOM_SAMPLING_METHOD,
        default_file_format=FILE_FORMAT_CSV,
        default_sampling_conf=SamplingConf(
            fraction="0.5",
            with_replacement=True,
            seed=SEED,
        ),
        default_format_conf=FileFormatConf(with_header=True, sep=","),
    )

    response = grpc_stub.RelationSamplingJob(request)
    assert response.code == 0


def test_chain_relation(grpc_stub):
    sampling_a = RelationSamplingRequest.SamplingStage(
        input_path=input_a,
        output_path=os.path.join(dir_prefix, "./output/chain-relation/a-sampled"),
        name="a",
    )
    relation_b = RelationSamplingRequest.RelationStage(
        input_path=input_b,
        output_path=os.path.join(dir_prefix, "./output/chain-relation/b-sampled"),
        name="b",
        relations=[
            RelationSamplingRequest.Relation(
                relation_name="a", relation_col="user_id", input_col="user_id"
            )
        ],
    )

    relation_c = RelationSamplingRequest.RelationStage(
        input_path=input_c,
        output_path=os.path.join(dir_prefix, "./output/chain-relation/c-sampled"),
        name="c",
        relations=[
            RelationSamplingRequest.Relation(
                relation_name="b", relation_col="session_id", input_col="session_id"
            )
        ],
    )

    request = RelationSamplingRequest(
        sampling_stages=[sampling_a],
        relation_stages=[relation_b, relation_c],
        job_id="chain-relation-job",
        default_sampling_method=RANDOM_SAMPLING_METHOD,
        default_file_format=FILE_FORMAT_CSV,
        default_sampling_conf=SamplingConf(
            fraction="0.5",
            with_replacement=True,
            seed=SEED,
        ),
        default_format_conf=FileFormatConf(with_header=True, sep=","),
    )

    response = grpc_stub.RelationSamplingJob(request)
    assert response.code == 0


def test_dry_run(grpc_stub):
    out_path = os.path.join(dir_prefix, "./output/dry/a-sampled")
    sampling_a = RelationSamplingRequest.SamplingStage(
        input_path=input_a,
        output_path=out_path,
        name="a",
    )

    request = RelationSamplingRequest(
        sampling_stages=[sampling_a],
        relation_stages=[],
        job_id="dry-run-job",
        default_sampling_method=RANDOM_SAMPLING_METHOD,
        default_file_format=FILE_FORMAT_CSV,
        default_sampling_conf=SamplingConf(
            fraction="0.5",
            with_replacement=True,
            seed=SEED,
        ),
        default_format_conf=FileFormatConf(with_header=True, sep=","),
        dry_run=True,
    )

    response = grpc_stub.RelationSamplingJob(request)
    assert response.code == 0
    assert not os.path.exists(out_path)


if __name__ == "__main__":
    pytest.main()
