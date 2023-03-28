import json
import os
import random
from typing import Dict

from pyspark.sql.functions import col, rand, row_number
from pyspark.sql.window import Window

from sparksampling.sample.base_sampling import SparkBaseSamplingJob


class StratifiedSamplingImp(SparkBaseSamplingJob):
    cls_args = [
        "fraction",
        "with_replacement",
        "stratified_key",
        "seed",
        "count",
        "sampling_col",
        "ensure_col",
    ]

    def __init__(self, *args, **kwargs):
        super(StratifiedSamplingImp, self).__init__(*args, **kwargs)
        self.fraction = self._init_fraction(kwargs.pop("fraction"))
        self.stratified_key = kwargs.pop("stratified_key")
        self.with_replacement = kwargs.pop("with_replacement", False)
        self.seed = kwargs.pop("seed", random.randint(1, 65535))
        self.count = kwargs.pop("count", 0)
        self.sampling_col = list(kwargs.pop("sampling_col", []))
        self.ensure_col = os.environ.get("FORCE_STRATIFILED_ENSURE_COL") in [
            "True",
            "true",
        ] or kwargs.pop("ensure_col", False)

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
        sampled_df = df.sampleBy(col=self.stratified_key, fractions=fraction, seed=self.seed)
        if self.ensure_col:
            self.log.info("ensure every layer in result, ignore count")
            sampled_df = self.sample_full_layer_df(sampled_df, df, self.stratified_key)
        if self.count:
            sampled_df = sampled_df.limit(self.count)
        return sampled_df

    @staticmethod
    def _convert_fraction_to_dict(df, stratified_key, fraction) -> Dict:
        if isinstance(fraction, Dict):
            return fraction

        # https://stackoverflow.com/questions/44367019/column-name-with-dot-spark
        # Prevent .(dot) breaking select
        StratifiedSamplingImp.log.info(f"Convert fraction {fraction} to dict...")
        y = df.select(f"`{stratified_key}`")
        convert_fraction = {label[0]: fraction for label in y.distinct().toLocalIterator()}
        return convert_fraction

    @staticmethod
    def sample_full_layer_df(sampled_df, origin_df, stratified_key):
        sampled_layer_key = sampled_df.select(f"`{stratified_key}`").distinct()
        missing_layer_key = (
            origin_df.select(f"`{stratified_key}`").distinct().exceptAll(sampled_layer_key)
        )

        missing_df = origin_df.join(missing_layer_key, stratified_key, "semi")
        stratified_window = Window.partitionBy(f"`{stratified_key}`").orderBy(rand())
        missing_layer = (
            missing_df.withColumn("_", row_number().over(stratified_window))
            .filter(col("_") == 1)
            .drop("_")
        )
        return sampled_df.unionByName(missing_layer, allowMissingColumns=True)
