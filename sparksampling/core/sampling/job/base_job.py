from sparksampling.core import CheckLogger
from pyspark.sql import DataFrame


class BaseJob(CheckLogger):
    def _generate(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        raise NotImplementedError

    def generate(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        self.logger.info("Generate Sampling Job...")
        return self._generate(df, *args, **kwargs)
