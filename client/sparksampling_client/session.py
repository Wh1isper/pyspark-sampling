import json

import grpc
from sparksampling_client.config import load_config
from sparksampling_client.proto import sampling_service_pb2_grpc
from sparksampling_client.proto.sampling_service_pb2 import SamplingRequest


class Session:
    def __init__(self, host=None, port=None):
        config = load_config()
        self.host = host or config.get("host")
        self.port = port or config.get("port")

    def make_request(self, request):
        with grpc.insecure_channel(f"{self.host}:{self.port}") as channel:
            stub = sampling_service_pb2_grpc.SparkSamplingServiceStub(channel)
            response = stub.SamplingJob(request)
        return response

    def apply_file(self, json_file):
        with open(json_file) as f:
            request = SamplingRequest(**json.load(f))
        return self.make_request(request)
