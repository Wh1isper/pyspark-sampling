import findspark

findspark.init()

import pytest
from sparksampling.app import SparkSamplingAPP


@pytest.fixture(scope='module')
def grpc_add_to_server():
    from sparksampling.proto.sampling_service_pb2_grpc import add_SparkSamplingServiceServicer_to_server
    from sparksampling.service import GRPCService

    GRPCService.get_worker_num()

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


@pytest.fixture(scope='module', autouse=True)
def register():
    from sparksampling.engine_factory import EngineFactory

    EngineFactory.register_all_engine()
