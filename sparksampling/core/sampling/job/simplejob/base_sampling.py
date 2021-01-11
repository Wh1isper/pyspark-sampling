import random
from pyspark.sql import DataFrame
from sparksampling.core.sampling.job.base_job import BaseJob


class BaseSamplingJob(BaseJob):
    type_map = {}

    def __init__(self, with_replacement=True, fraction: str or dict = None, seed=random.randint(1, 65535), col_key=None):
        self.with_replacement = with_replacement
        self.fraction = float(fraction) if type(fraction) is str else fraction
        self.seed = seed
        self.col_key = col_key
        self.check_type()
