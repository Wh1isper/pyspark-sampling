from pyspark.sql import DataFrame
from sparksampling.core.sampling.job.base_job import BaseJob
import random
from sparksampling.core.mlsamplinglib import pre_smote_df_process, smote, restore_smoted_df
from sparksampling.core.mlsamplinglib.func import get_num_cat_feat, df_with_column_int
from pyspark.sql.functions import col

class SmoteSamplingJob(BaseJob):
    type_map = {
        'seed': int,
        'bucket_length': int,
        'k': int,
        'multiplier': int,
        'restore': bool,
        'col_key': str,
    }

    def __init__(self, k=3, multiplier=2, bucket_length=10, seed=random.randint(1, 65535), restore=True, col_key=None):
        super(SmoteSamplingJob, self).__init__()
        self.k = k
        self.bucket_length = bucket_length
        self.multiplier = multiplier
        self.seed = seed
        self.restore = bool(restore)
        self.col_key = col_key
        self.check_type()

    def _generate(self, df: DataFrame, *args, **kwargs) -> DataFrame:
        df = df_with_column_int(df)
        num_cols, cat_cols = get_num_cat_feat(df)
        vectorized, smote_stages = pre_smote_df_process(df, num_cols, cat_cols, self.col_key, False)
        smoted_train_df = smote(vectorized, self)
        if self.restore:
            return restore_smoted_df(num_cols, smoted_train_df, 'features')
        else:
            return smoted_train_df
