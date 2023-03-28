from sparksampling.engine_factory import EngineFactory
from sparksampling.mixin import LogMixin
from sparksampling.proto import sampling_service_pb2_grpc
from sparksampling.proto.sampling_service_pb2 import (
    CancelRequest,
    CancelResponse,
    RelationSamplingRequest,
    RelationSamplingResponse,
    SamplingRequest,
    SamplingResponse,
)
from sparksampling.utilities import throw_exception


class GRPCService(sampling_service_pb2_grpc.SparkSamplingServiceServicer, LogMixin):
    def __init__(self, parent):
        self.parent = parent

    @staticmethod
    def register_engine(parent):
        EngineFactory.register_all_engine()

    @staticmethod
    def get_worker_num():
        return EngineFactory.get_engine_total_worker()

    @staticmethod
    def add_to_server(parent, server):
        """Register the service to the GRPC Server
        :param parent: application instance
        :param server: server instance
        :return:
        """
        GRPCService.logger.info("Add GRPCService Service to server")
        sampling_service_pb2_grpc.add_SparkSamplingServiceServicer_to_server(
            GRPCService(parent), server
        )

    @throw_exception
    def SamplingJob(self, request: SamplingRequest, context) -> SamplingResponse:
        data = EngineFactory.message_to_dict(request)
        parent_request = SamplingRequest(**data)
        engine = EngineFactory.get_engine(self.parent, SamplingRequest, **data)
        output_path, hook_msg = engine.submit()
        return SamplingResponse(
            code=0,
            message="",
            data=SamplingResponse.ResponseData(
                parent_request=parent_request, sampled_path=output_path, hook_msg=hook_msg
            ),
        )

    @throw_exception
    def RelationSamplingJob(
        self, request: RelationSamplingRequest, context
    ) -> RelationSamplingResponse:
        data = EngineFactory.message_to_dict(request)
        parent_request = RelationSamplingRequest(**data)
        engine = EngineFactory.get_engine(self.parent, RelationSamplingRequest, **data)
        results = engine.submit()

        return RelationSamplingResponse(
            code=0,
            message="",
            data=RelationSamplingResponse.ResponseData(
                parent_request=parent_request, results=results
            ),
        )

    def CancelJob(self, request: CancelRequest, context) -> CancelResponse:
        try:
            EngineFactory.cancel_job(self.parent, request.job_id)
        except Exception as e:
            self.logger.exception(e)
            return CancelResponse(code=5000, message=str(e))
        return CancelResponse(code=0, message="ok")
