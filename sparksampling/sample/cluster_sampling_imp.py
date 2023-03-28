import math
import random

from sparksampling.sample.base_sampling import SparkBaseSamplingJob


class ClusterSamplingImp(SparkBaseSamplingJob):
    cls_args = ["fraction", "seed", "count", "sampling_col", "group_by", "group_num"]

    def __init__(self, *args, **kwargs):
        super(ClusterSamplingImp, self).__init__(*args, **kwargs)
        self.fraction = float(kwargs.pop("fraction", 0))
        self.seed = kwargs.pop("seed", random.randint(1, 65535))
        self.count = kwargs.pop("count", 0)
        self.sampling_col = list(kwargs.pop("sampling_col", []))
        self.group_by = str(kwargs.pop("group_by"))
        self.group_num = int(kwargs.pop("group_num", 0))

    def run(self, df):
        if self.sampling_col:
            df = df[self.sampling_col]

        # https://stackoverflow.com/questions/44367019/column-name-with-dot-spark
        # Prevent .(dot) breaking select
        group = df.select(f"`{self.group_by}`").distinct()
        group_sampled = self.get_group_sampled(group)

        # equal to df.join(group_sampled, df.group_by == group_sampled.group_by, 'semi'), this will keep column in order
        cond = [getattr(df, self.group_by) == getattr(group_sampled, self.group_by)]
        df = df.join(group_sampled, cond, how="semi")
        if self.count:
            df = df.limit(self.count)

        return df

    def get_group_sampled(self, group):
        if self.fraction:
            self.log.info(f"Using fraction:{self.fraction} to sample group")
            group_df = group.sample(fraction=self.fraction, withReplacement=False, seed=self.seed)
            if self.group_num:
                self.log.info(
                    f"Fraction with group_num, limit group as group_num: {self.group_num}"
                )
                group_df = group_df.limit(self.group_num)
        else:
            # Cluster sampling expects to get the exact number of groups
            self.log.info(
                f"No fraction specified, using rdd.takeSample to get the exact number of groups"
            )
            subset = group.rdd.takeSample(False, self.group_num, seed=self.seed)
            try:
                group_df = self.spark.sparkContext.parallelize(subset).toDF()
            except ValueError as e:
                self.log.info(
                    f"Exception when rdd -> df, {e}, will use group df without being sampled"
                )
                self.log.exception(e)
                group_df = group
        return group_df
