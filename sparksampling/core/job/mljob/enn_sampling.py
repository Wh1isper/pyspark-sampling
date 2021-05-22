from pyspark.sql import DataFrame
from sparksampling.core.job.base_job import BaseJob
from sparksampling.core.mlsamplinglib import SparkEditedNearestNeighbours


class SparkENNSamplingJob(BaseJob):
    type_map = {
        'n_neighbors': int,
        'drop_list': list,
        'col_key': str
    }

    def __init__(self, n_neighbors=3, drop_list=None, col_key=None):
        super(SparkENNSamplingJob, self).__init__()
        if drop_list is None:
            drop_list = []
        self.n_neighbors = n_neighbors
        self.drop_list = drop_list
        self.col_key = col_key
        self.check_type()

    def _generate(self, df: DataFrame, *args, **kwargs) -> DataFrame:

        y = df.select(self.col_key)
        if self.col_key not in self.drop_list:
            self.drop_list.append(self.col_key)
        x = df.drop(*self.drop_list)
        enn = SparkEditedNearestNeighbours(n_neighbors=self.n_neighbors)
        return enn.fit_resample(x, y)
