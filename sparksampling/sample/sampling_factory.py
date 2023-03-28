from sparksampling.error import BadParamError
from sparksampling.proto.sampling_service_pb2 import (
    CLUSTER_SAMPLING_METHOD,
    RANDOM_SAMPLING_METHOD,
    SIMPLE_SAMPLING_METHOD,
    STRATIFIED_SAMPLING_METHOD,
)
from sparksampling.sample.base_sampling import SparkBaseSamplingJob
from sparksampling.sample.cluster_sampling_imp import ClusterSamplingImp
from sparksampling.sample.random_sampling_imp import RandomSamplingImp
from sparksampling.sample.simple_sampling_imp import SimpleSamplingImp
from sparksampling.sample.stratified_sampling_imp import StratifiedSamplingImp


class SamplingFactory(object):
    method_map = {
        RANDOM_SAMPLING_METHOD: RandomSamplingImp,
        STRATIFIED_SAMPLING_METHOD: StratifiedSamplingImp,
        SIMPLE_SAMPLING_METHOD: SimpleSamplingImp,
        CLUSTER_SAMPLING_METHOD: ClusterSamplingImp,
    }

    @classmethod
    def get_sampling_imp(cls, sampling_method, sampling_conf) -> SparkBaseSamplingJob:
        return cls._get_imp_class(sampling_method)(
            **cls._get_sampling_conf(sampling_method, sampling_conf)
        )

    @classmethod
    def _get_imp_class(cls, sampling_method):
        imp_class = cls.method_map.get(sampling_method, None)
        if not imp_class:
            raise BadParamError(f"No matching sampling method: {sampling_method}")
        return imp_class

    @classmethod
    def _get_sampling_conf(cls, sampling_method, sampling_conf):
        return cls.method_map[sampling_method].get_init_conf(sampling_conf)
