"""
adapter for spark
"""
from pyspark.sql import DataFrame
from sparksampling.core.sampling.base import Logger
import random


class BaseSamplingJob(Logger):
    type_map = {}

    def __init__(self, with_replacement=True, fraction: str or dict = None, seed=random.randint(1, 65535), key=None):
        self.with_replacement = with_replacement
        self.fraction = float(fraction) if type(fraction) is str else fraction
        self.seed = seed
        self.key = key
        self.check_type()

    def _generate(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        raise NotImplementedError

    def check_type(self):
        for attr, atype in self.type_map.items():
            if type(getattr(self, attr)) is not atype:
                raise TypeError(
                    f"Expected {attr} as {atype.__name__}, got {type(getattr(self, attr)).__name__} instead.")

    def generate(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        self.logger.info("Generate Sampling Job...")
        return self._generate(df, *args, **kwargs)


class SimpleRandomSamplingJob(BaseSamplingJob):
    type_map = {
        'with_replacement': bool,
        'fraction': float,
        'seed': int
    }

    def __init__(self, *args, **kwargs):
        super(SimpleRandomSamplingJob, self).__init__(*args, **kwargs)

    def _generate(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        return df.sample(withReplacement=self.with_replacement, fraction=self.fraction, seed=self.seed)


class StratifiedSamplingJob(BaseSamplingJob):
    type_map = {
        'key': str,
        'fraction': dict
    }

    def __init__(self, *args, **kwargs):
        super(StratifiedSamplingJob, self).__init__(*args, **kwargs)

    def _generate(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        return df.sampleBy(col=self.key, fractions=self.fraction, seed=self.seed)


class SmoteSamplingJob(BaseSamplingJob):
    ...
