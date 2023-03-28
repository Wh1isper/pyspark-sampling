import random

from sparksampling.sample.base_sampling import SparkBaseSamplingJob


class SimpleSamplingImp(SparkBaseSamplingJob):
    cls_args = ["fraction", "with_replacement", "seed", "count", "sampling_col"]

    def __init__(self, *args, **kwargs):
        super(SimpleSamplingImp, self).__init__(*args, **kwargs)
        self.fraction = float(kwargs.pop("fraction", 1))
        self.with_replacement = kwargs.pop("with_replacement", False)
        self.seed = kwargs.pop("seed", random.randint(1, 65535))
        self.count = kwargs.pop("count")
        self.sampling_col = list(kwargs.pop("sampling_col", []))

    def run(self, df):
        if self.sampling_col:
            df = df[self.sampling_col]
        return df.sample(
            fraction=self.fraction, seed=self.seed, withReplacement=self.with_replacement
        ).limit(self.count)
