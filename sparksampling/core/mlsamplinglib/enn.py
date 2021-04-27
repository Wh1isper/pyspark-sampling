from pyspark.ml.feature import BucketedRandomProjectionLSH
import numpy as np
from pyspark.sql import DataFrame, Window

from sparksampling.core.mlsamplinglib.func import vectorized_feature
import pyspark.sql.functions as F


class SparkEditedNearestNeighbours(object):
    def __init__(self, n_neighbors):
        self.n_neighbors = n_neighbors

    def fit_resample(self, x: DataFrame, y: DataFrame) -> DataFrame:
        vectorized = vectorized_feature(x)
        vectorized = vectorized.withColumn("index", F.monotonically_increasing_id())
        y = y.withColumn("index", F.monotonically_increasing_id())

        brp = BucketedRandomProjectionLSH(inputCol="features", outputCol="hashes", seed=np.random.randint(1, 65535),
                                          bucketLength=3)
        # smote only applies on existing minority instances
        model = brp.fit(vectorized)
        model.transform(vectorized)

        # here distance is calculated from brp's param inputCol
        self_join_w_distance = model.approxSimilarityJoin(vectorized, vectorized, float("inf"),
                                                          distCol="EuclideanDistance")

        # remove self-comparison (distance 0)
        self_join_w_distance = self_join_w_distance.filter(self_join_w_distance.EuclideanDistance > 0)

        over_original_rows = Window.partitionBy("datasetA").orderBy("EuclideanDistance")
        self_similarity_df = self_join_w_distance.withColumn("r_num", F.row_number().over(over_original_rows))
        # topK
        self_similarity_df_selected = self_similarity_df.filter(self_similarity_df.r_num <= self.n_neighbors)
        self_similarity_df_selected = self_similarity_df_selected.withColumn("index_a",
                                                                             self_similarity_df_selected.datasetA.index)
        self_similarity_df_selected = self_similarity_df_selected.withColumn("index_b",
                                                                             self_similarity_df_selected.datasetB.index)
        # vote
        distance_with_label = self_similarity_df_selected.join(y, self_similarity_df_selected.index_a == y.index)
        distance_with_label = distance_with_label.withColumnRenamed('y', 'true_label').drop('index')
        distance_with_label = distance_with_label.join(y, distance_with_label.index_b == y.index)
        distance_with_label = distance_with_label.withColumnRenamed('y', 'pre_label').drop('index')
        incorrect_matrix = distance_with_label.filter(
            distance_with_label.true_label != distance_with_label.pre_label)
        incorrect_count = Window.partitionBy("datasetA").orderBy("EuclideanDistance")
        count_matrix = incorrect_matrix.withColumn("incorrect_count", F.row_number().over(incorrect_count))
        index_to_remove = count_matrix.filter(count_matrix.incorrect_count > self.n_neighbors // 2).select(
            'index_a').distinct().withColumnRenamed('index_a', 'index')

        # reduce
        output_x = vectorized.join(index_to_remove, vectorized.index == index_to_remove.index, 'anti').drop(
            'features').drop(
            index_to_remove.index)
        output_y = y.join(index_to_remove, y.index == index_to_remove.index, 'anti').drop(index_to_remove.index)
        output = output_x.join(output_y, output_x.index == output_y.index).drop('index')
        return output
