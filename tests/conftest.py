import findspark

findspark.init()

import pytest
from sparksampling.app import SparkSamplingAPP


@pytest.fixture(scope='module')
def grpc_add_to_server():
    from sparksampling.proto.sampling_service_pb2_grpc import add_SparkSamplingServiceServicer_to_server
    from sparksampling.service import GRPCService

    GRPCService.guarantee_worker = 1
    return add_SparkSamplingServiceServicer_to_server


@pytest.fixture(scope='module')
def grpc_servicer():
    from sparksampling.service import GRPCService

    SparkSamplingAPP().initialize()

    return GRPCService(SparkSamplingAPP())


@pytest.fixture(scope='module')
def grpc_stub_cls(grpc_channel):
    from sparksampling.proto.sampling_service_pb2_grpc import SparkSamplingServiceStub

    return SparkSamplingServiceStub
