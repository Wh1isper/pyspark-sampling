from sparksampling.sample.base_sampling import SparkBaseSamplingJob
import random


class RandomSamplingImp(SparkBaseSamplingJob):
    cls_args = ['fraction', 'with_replacement', 'seed', 'count', 'sampling_col']

    def __init__(self, *args, **kwargs):
        super(RandomSamplingImp, self).__init__(*args, **kwargs)
        self.fraction = float(kwargs.pop('fraction'))
        self.with_replacement = kwargs.pop('with_replacement', False)
        self.seed = kwargs.pop('seed', random.randint(1, 65535))
        self.count = kwargs.pop('count', 0)
        self.sampling_col = list(kwargs.pop('sampling_col', []))

    def run(self, df):
        if self.sampling_col:
            df = df[self.sampling_col]
        df = df.sample(fraction=self.fraction, seed=self.seed, withReplacement=self.with_replacement)
        if self.count:
            df = df.limit(self.count)
        return df
