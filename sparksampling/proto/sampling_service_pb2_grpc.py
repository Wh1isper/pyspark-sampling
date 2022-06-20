# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from . import sampling_service_pb2 as sampling__service__pb2


class SparkSamplingServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.SamplingJob = channel.unary_unary(
                '/SparkSamplingService/SamplingJob',
                request_serializer=sampling__service__pb2.SamplingRequest.SerializeToString,
                response_deserializer=sampling__service__pb2.SamplingResponse.FromString,
                )
        self.RelationSamplingJob = channel.unary_unary(
                '/SparkSamplingService/RelationSamplingJob',
                request_serializer=sampling__service__pb2.RelationSamplingRequest.SerializeToString,
                response_deserializer=sampling__service__pb2.RelationSamplingResponse.FromString,
                )
        self.CancelJob = channel.unary_unary(
                '/SparkSamplingService/CancelJob',
                request_serializer=sampling__service__pb2.CancelRequest.SerializeToString,
                response_deserializer=sampling__service__pb2.CancelResponse.FromString,
                )


class SparkSamplingServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def SamplingJob(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def RelationSamplingJob(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def CancelJob(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_SparkSamplingServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'SamplingJob': grpc.unary_unary_rpc_method_handler(
                    servicer.SamplingJob,
                    request_deserializer=sampling__service__pb2.SamplingRequest.FromString,
                    response_serializer=sampling__service__pb2.SamplingResponse.SerializeToString,
            ),
            'RelationSamplingJob': grpc.unary_unary_rpc_method_handler(
                    servicer.RelationSamplingJob,
                    request_deserializer=sampling__service__pb2.RelationSamplingRequest.FromString,
                    response_serializer=sampling__service__pb2.RelationSamplingResponse.SerializeToString,
            ),
            'CancelJob': grpc.unary_unary_rpc_method_handler(
                    servicer.CancelJob,
                    request_deserializer=sampling__service__pb2.CancelRequest.FromString,
                    response_serializer=sampling__service__pb2.CancelResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'SparkSamplingService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class SparkSamplingService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def SamplingJob(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/SparkSamplingService/SamplingJob',
            sampling__service__pb2.SamplingRequest.SerializeToString,
            sampling__service__pb2.SamplingResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def RelationSamplingJob(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/SparkSamplingService/RelationSamplingJob',
            sampling__service__pb2.RelationSamplingRequest.SerializeToString,
            sampling__service__pb2.RelationSamplingResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def CancelJob(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/SparkSamplingService/CancelJob',
            sampling__service__pb2.CancelRequest.SerializeToString,
            sampling__service__pb2.CancelResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)