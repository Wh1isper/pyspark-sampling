from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import EditedNearestNeighbours as ENN
from pyspark.sql import DataFrame
from imblearn.combine import SMOTEENN
import pandas as pd

from sparksampling.core.job.base_job import BaseJob
from sparksampling.utilities.utilities import pandas_to_spark


class ImbSMOTEENNSamplingJob(BaseJob):
    type_map = {
        'k_neighbors': int,
        'n_neighbors': int,
        'drop_list': list,
        'col_key': str
    }

    def __init__(self, k_neighbors=3, n_neighbors=3, drop_list=None, col_key=None):
        super(ImbSMOTEENNSamplingJob, self).__init__()
        if drop_list is None:
            drop_list = []
        self.k_neighbors = k_neighbors
        self.n_neighbors = n_neighbors
        self.drop_list = drop_list
        self.col_key = col_key
        self.check_type()

    def _generate(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        df = df.toPandas()
        y = df[[self.col_key]]
        if self.col_key not in self.drop_list:
            self.drop_list.append(self.col_key)
        x = df.drop(self.drop_list, axis=1)
        smoteenn = SMOTEENN(smote=SMOTE(k_neighbors=self.k_neighbors),
                            enn=ENN(n_neighbors=self.n_neighbors, sampling_strategy="all"))
        x_fit, y_fit = smoteenn.fit_resample(x.values, y.values)
        result_df = pd.concat([pd.DataFrame(x_fit, columns=x.columns), pd.DataFrame(y_fit, columns=y.columns)], axis=1)
        return pandas_to_spark(result_df)
