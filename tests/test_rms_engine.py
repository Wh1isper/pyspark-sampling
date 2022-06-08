import os

from sparksampling.proto.sampling_service_pb2 import RelationSamplingRequest, SamplingConf, FileFormatConf, \
    RANDOM_SAMPLING_METHOD, FILE_FORMAT_CSV

dir_prefix = os.path.abspath(os.path.dirname(__file__))
input_path = os.path.join(dir_prefix, '../data/10w_x_10.csv')


def setup_module():
    import shutil

    shutil.rmtree(os.path.join(dir_prefix, './output/'), ignore_errors=True)
    shutil.rmtree(os.path.join(dir_prefix, '../data/sampled'), ignore_errors=True)


def test_abc_relation(grpc_stub):
    sampling_a = RelationSamplingRequest.SamplingStage(
        input_path=os.path.join(dir_prefix, '../data/relation-a.csv'),
        output_path=os.path.join(dir_prefix, './output/relation-a-sampled'),
        name='a',
    )
    relation_b = RelationSamplingRequest.RelationStage(
        input_path=os.path.join(dir_prefix, '../data/relation-b.csv'),
        output_path=os.path.join(dir_prefix, './output/relation-b-sampled'),
        name='b',
        relation=[
            RelationSamplingRequest.Relation(
                relation_name='a',
                relation_col='user_id',
                input_col='user_id'
            )
        ],
    )

    relation_c = RelationSamplingRequest.RelationStage(
        input_path=os.path.join(dir_prefix, '../data/relation-c.csv'),
        output_path=os.path.join(dir_prefix, './output/relation-c-sampled'),
        name='c',
        relation=[
            RelationSamplingRequest.Relation(
                relation_name='b',
                relation_col='session_id',
                input_col='session_id'
            )
        ],
    )

    request = RelationSamplingRequest(
        sampling_stages=[sampling_a],
        relation_stages=[relation_b, relation_c],
        job_id='abc-relation-job',
        default_sampling_method=RANDOM_SAMPLING_METHOD,
        default_file_format=FILE_FORMAT_CSV,
        default_sampling_conf=SamplingConf(
            fraction='0.5',
            with_replacement=True,
        ),
        default_format_conf=FileFormatConf(
            with_header=False,
            sep=','
        ),
    )

    response = grpc_stub.RelationSamplingJob(request)
    assert response.code == 0
