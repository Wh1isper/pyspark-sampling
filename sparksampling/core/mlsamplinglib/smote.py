import random
from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.ml.linalg import VectorUDT
from pyspark.ml.feature import BucketedRandomProjectionLSH
import numpy as np

from sparksampling.core.mlsamplinglib.func import vectorized_feature


class SparkSMOTE(object):
    def __init__(self, k_neighbors):
        self.k_neighbors = k_neighbors

    def fit_resample(self, x: DataFrame, y: DataFrame) -> DataFrame:
        vectorized = vectorized_feature(x)
        vectorized = vectorized.withColumn("index", F.monotonically_increasing_id())
        min_label, t_round = self.get_sample_label_and_t_round(y)
        y = y.withColumn("index", F.monotonically_increasing_id())
        label_index = self.get_label_index(y)
        vectorized_sdf = vectorized.join(y, vectorized.index == y.index).drop("index")
        df_min = vectorized_sdf.filter(f"{label_index} == {min_label}")

        brp = BucketedRandomProjectionLSH(inputCol="features", outputCol="hashes", seed=np.random.randint(1, 65535),
                                          bucketLength=3)
        # smote only applies on existing minority instances
        model = brp.fit(df_min)
        model.transform(df_min)

        # here distance is calculated from brp's param inputCol
        self_join_w_distance = model.approxSimilarityJoin(df_min, df_min, float("inf"),
                                                          distCol="EuclideanDistance")

        # remove self-comparison (distance 0)
        self_join_w_distance = self_join_w_distance.filter(self_join_w_distance.EuclideanDistance > 0)
        over_original_rows = Window.partitionBy("datasetA").orderBy("EuclideanDistance")
        self_similarity_df = self_join_w_distance.withColumn("r_num", F.row_number().over(over_original_rows))
        self_similarity_df_selected = self_similarity_df.filter(self_similarity_df.r_num <= self.k_neighbors)

        over_original_rows_no_order = Window.partitionBy('datasetA')
        original_cols = df_min.columns
        subtract_vector_udf = F.udf(lambda arr: random.uniform(0, 1) * (arr[0] - arr[1]), VectorUDT())
        add_vector_udf = F.udf(lambda arr: arr[0] + arr[1], VectorUDT())
        res = []
        for _ in range(t_round):
            df_random_sel = self_similarity_df_selected.withColumn("rand", F.rand()).withColumn('max_rand',
                                                                                                F.max('rand').over(
                                                                                                    over_original_rows_no_order)) \
                .where(F.col('rand') == F.col('max_rand')).drop(*['max_rand', 'rand', 'r_num'])
            df_vec_diff = df_random_sel.select('*',
                                               subtract_vector_udf(
                                                   F.array('datasetA.features', 'datasetB.features')).alias(
                                                   'vec_diff'))
            df_vec_modified = df_vec_diff.select('*',
                                                 add_vector_udf(F.array('datasetA.features', 'vec_diff')).alias(
                                                     'features'))

            for c in original_cols:
                # randomly select neighbour or original data
                col_sub = random.choice(['datasetA', 'datasetB'])
                val = "{0}.{1}".format(col_sub, c)
                if c != 'features':
                    # do not unpack original numerical features
                    df_vec_modified = df_vec_modified.withColumn(c, F.col(val))

            # this df_vec_modified is the synthetic minority instances,
            df_vec_modified = df_vec_modified.drop(
                *['features', 'datasetA', 'datasetB', 'vec_diff', 'EuclideanDistance'])

            res.append(df_vec_modified)
        dfunion = reduce(DataFrame.unionAll, res)
        oversampled_df = dfunion.union(vectorized_sdf.select(dfunion.columns))
        return oversampled_df

    def get_sample_label_and_t_round(self, y: DataFrame) -> (str, int):
        labels = y.distinct().toPandas().to_numpy().reshape(-1)
        label_index = y.columns[0]
        count_map = {}
        for label in labels:
            count_map[label] = y.filter(f"{label_index} == {label}").count()
        max_num = count_map[max(count_map, key=count_map.get)]
        min_num = count_map[min(count_map, key=count_map.get)]
        t_count = (max_num - min_num) // min_num

        return min(count_map, key=count_map.get), t_count

    def get_label_index(self, y: DataFrame):
        return y.columns[0]
