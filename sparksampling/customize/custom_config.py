extra_statistics_job = {

}

extra_evaluation_job = {

}

extra_sampling_job = {

}

extra_dataio = {

}

from sparksampling.core.dataio.base_dataio import BaseDataIO
from sparksampling.core.sampling.job.base_job import BaseJob
from pyspark.sql import DataFrame
import pandas as pd


class DummyJob(BaseJob):
    def _generate(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        return df

    def _statistics(self, df: DataFrame, *args, **kwargs) -> dict:
        return {'auc': 888}


class DummyDataIO(BaseDataIO):
    def _read(self, header=True, *args, **kwargs) -> DataFrame:
        pandas_df = pd.read_csv("this is not a path")
        return self.spark.createDataFrame(pandas_df)

    def _write(self, *args, **kwargs):
        return
