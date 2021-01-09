from pyspark.sql import DataFrame
from sparksampling.core.sampling.job.simplejob.base_sampling import BaseSamplingJob


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
