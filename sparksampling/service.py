from sparksampling.engine.engine_factory import EngineFactory
from sparksampling.mixin import WorkerManagerMixin, acquire_worker
from sparksampling.proto.sampling_service_pb2 import (
    SamplingResponse,
    SamplingRequest,
    CancelRequest,
    CancelResponse
)
from sparksampling.proto import sampling_service_pb2_grpc
from sparksampling.utilities import except_execute


class GRPCService(sampling_service_pb2_grpc.SparkSamplingServiceServicer, WorkerManagerMixin):

    def __init__(self, parent):
        self.parent = parent

    @staticmethod
    def add_to_server(parent, server, guarantee_worker=0):
        """注册该服务到server中
        :param parent: application实例
        :param server: server实例
        :return:
        """
        GRPCService.logger.info('add JobService to server')
        sampling_service_pb2_grpc.add_SparkSamplingServiceServicer_to_server(
            GRPCService(parent), server)
        GRPCService.guarantee_worker = guarantee_worker
        if GRPCService.guarantee_worker <= 0:
            GRPCService.logger.warning("!!!no worker for sample job")
        else:
            GRPCService.logger.info(f"service has {guarantee_worker} workers for sample job")

    @except_execute
    @acquire_worker
    def SamplingJob(self, request: SamplingRequest, context) -> SamplingResponse:
        """启动实验
        """
        data = EngineFactory.message_to_dict(request)
        parent_request = SamplingRequest(**data)
        engine = EngineFactory.get_engine(self.parent, **data)
        output_path = engine.submit()
        return SamplingResponse(code=0, message='',
                                data=SamplingResponse.ResponseData(parent_request=parent_request,
                                                                   sampled_path=output_path))

    def CancelJob(self, request: CancelRequest, context) -> CancelResponse:
        try:
            EngineFactory.cancel_job(self.parent, request.job_id)
        except Exception as e:
            self.logger.exception(e)
            return CancelResponse(code=5000, message=str(e))
        return CancelResponse(code=0, message='ok')
