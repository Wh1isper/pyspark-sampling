from pyspark.sql import DataFrame
from sparksampling.core.sampling.job.simplejob.base_sampling import BaseSamplingJob


class StratifiedSamplingJob(BaseSamplingJob):
    type_map = {
        'key': str,
        'fraction': dict
    }

    def __init__(self, *args, **kwargs):
        super(StratifiedSamplingJob, self).__init__(*args, **kwargs)

    def _generate(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        return df.sampleBy(col=self.key, fractions=self.fraction, seed=self.seed)
