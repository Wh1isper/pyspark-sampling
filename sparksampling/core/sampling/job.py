"""
adapter for spark
"""
from pyspark.sql import DataFrame


class SimpleRandomSamplingJob(object):
    def __init__(self, with_replacement=True, fraction=0.5, seed=1):
        self.with_replacement = with_replacement
        self.fraction = fraction
        self.seed = seed

    def generate(self, df: DataFrame):
        return df.sample(withReplacement=self.with_replacement, fraction=self.fraction, seed=self.seed)


class StratifiedSamplingJob(SimpleRandomSamplingJob):
    def __init__(self):
        super(StratifiedSamplingJob, self).__init__()
        ...
