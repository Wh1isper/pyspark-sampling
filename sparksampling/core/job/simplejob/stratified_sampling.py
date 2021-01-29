from pyspark.sql import DataFrame
from sparksampling.core.job.simplejob.base_sampling import BaseSamplingJob


class StratifiedSamplingJob(BaseSamplingJob):
    type_map = {
        'col_key': str,
        'fraction': dict,
        'seed': int
    }

    def __init__(self, *args, **kwargs):
        super(StratifiedSamplingJob, self).__init__(*args, **kwargs)
        self.check_type()

    def _generate(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        return df.sampleBy(col=self.col_key, fractions=self.fraction, seed=self.seed)
