from typing import Any

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import DataFrame
from sparksampling.core.job.base_job import BaseJob

import numpy as np
from pyspark.sql.types import DoubleType
from sparksampling.core.mlsamplinglib.func import vectorized_feature

import random


class KmeansEvaluationJob(BaseJob):
    type_map = {
        'source_path': str,
        'selected_features_list': list,
        'K': Any,
        'round': int,
        'key': str,
    }

    def __init__(self, source_path=None, selected_features_list=None, key=None, K=None, round=10, *args, **kwargs):
        super(KmeansEvaluationJob, self).__init__(*args, **kwargs)
        self.source_path = source_path
        self.selected_features_list = selected_features_list
        self.K = K
        self.round = round
        self.key = key
        self.check_type()

    def KM(self, dataset=None, K=2, predict=None, seed=np.random.randint(1, 65535)):
        # Trains a k-means model.
        kmeans = KMeans(seed=seed).setK(K).setFeaturesCol('features').setPredictionCol(
            'prediction')
        model = kmeans.fit(dataset)

        # Make predictions
        predictions = model.transform(predict)

        # Shows the result.
        centers = model.clusterCenters()
        return centers, predictions

    def evaluation_centers(self, centers, sample_centers):
        def metric_corrcoef_matrix(corrcoef_matrix):
            corrcoef = abs(corrcoef_matrix[0][1])
            score = int(corrcoef * 100)
            return score

        result = []
        for i in range(len(centers)):
            mix = np.array([centers[i], sample_centers[i]])
            corrcoef_matrix = np.corrcoef(mix)
            result.append(metric_corrcoef_matrix(corrcoef_matrix))
        return int(np.mean(result))

    def evaluation_prediction(self, prediction, sample_prediction):
        prediction = prediction.withColumnRenamed('prediction', 'label')
        df = sample_prediction.join(prediction, ['features'])
        df = df.select(['prediction', 'label']).withColumn("label", df.label.cast(DoubleType())).withColumn(
            "prediction", df.prediction.cast(DoubleType()))
        evaluator_accuracy = MulticlassClassificationEvaluator(predictionCol="prediction", metricName="accuracy",
                                                               labelCol='label')
        return evaluator_accuracy.evaluate(df)

    def _statistics(self, df: DataFrame, *args, **kwargs) -> dict:
        if not self.K:
            self.K = self.get_K_num_by_label(df)
        source_df = self._get_df_from_source(self.source_path, dataio=kwargs.get('data_io')).select(
            *self.selected_features_list)
        df = df.select(*self.selected_features_list)
        source_df = vectorized_feature(source_df)
        df = vectorized_feature(df)
        acc, cent, score = 0, 0, 0
        for _ in range(self.round):
            t_acc, t_cent = self.__kmeans_acc(source_df, df, *args, **kwargs)
            t_acc_score = int((t_acc * 100) ** (1 / 2) * 10)
            t_score = np.mean([t_acc_score, t_cent])
            if t_score > score:
                score, acc, cent = t_score, t_acc, t_cent
                print(f"acc: {t_acc_score}, cent: {t_cent}, score:{t_score}")
        return {
            "score": score,
            "accuracy": acc,
            "centers_result": cent,
        }

    def __kmeans_acc(self, source_df: DataFrame, df: DataFrame, *args, **kwargs):
        seed = np.random.randint(1, 65535)
        centers, predictions = self.KM(source_df, self.K, df, seed=seed)
        sample_centers, sample_predictions = self.KM(df, self.K, df, seed=seed)

        # 计算中心的相关系数
        centers_result = self.evaluation_centers(centers, sample_centers)

        # 计算抽样中的准确率召回率
        accuracy = self.evaluation_prediction(predictions, sample_predictions)
        return accuracy, centers_result

    def get_K_num_by_label(self, df: DataFrame):
        if not self.key:
            return 2
        print(f"Get K by label col:{self.key}")
        return df.select(self.key).distinct().count()
