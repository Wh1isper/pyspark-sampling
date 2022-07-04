from typing import Dict

from sparksampling.sample.base_sampling import SparkBaseSamplingJob
import random
import json


class StratifiedSamplingImp(SparkBaseSamplingJob):
    cls_args = ['fraction', 'with_replacement', 'stratified_key', 'seed', 'count', 'sampling_col']

    def __init__(self, *args, **kwargs):
        super(StratifiedSamplingImp, self).__init__(*args, **kwargs)
        self.fraction = self._init_fraction(kwargs.pop('fraction'))
        self.stratified_key = kwargs.pop('stratified_key')
        self.with_replacement = kwargs.pop('with_replacement', False)
        self.seed = kwargs.pop('seed', random.randint(1, 65535))
        self.count = kwargs.pop('count', 0)
        self.sampling_col = list(kwargs.pop('sampling_col', []))

    @staticmethod
    def _init_fraction(fraction_str):
        fraction = json.loads(fraction_str)
        if isinstance(fraction, int):
            fraction = float(fraction)
        if not isinstance(fraction, (Dict, float)):
            raise ValueError("fraction must be float or dict")
        return fraction

    def run(self, df):
        fraction = self._convert_fraction_to_dict(df, self.stratified_key, self.fraction)
        if self.sampling_col:
            df = df[self.sampling_col]
        df = df.sampleBy(col=self.stratified_key, fractions=fraction, seed=self.seed)
        if self.count:
            df = df.limit(self.count)
        return df

    @staticmethod
    def _convert_fraction_to_dict(df, stratified_key, fraction) -> Dict:
        if isinstance(fraction, Dict):
            return fraction

        # https://stackoverflow.com/questions/44367019/column-name-with-dot-spark
        # Prevent .(dot) breaking select
        y = df.select(f"`{stratified_key}`")
        labels = y.distinct().toPandas().to_numpy().reshape(-1)
        convert_fraction = dict()
        for label in labels:
            convert_fraction[label] = fraction
        StratifiedSamplingImp.log.info(f"Convert fraction {fraction} to dict", convert_fraction)
        return convert_fraction
