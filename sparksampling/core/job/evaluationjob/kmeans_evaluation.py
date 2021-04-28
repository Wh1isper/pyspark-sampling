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
        'K': Any
    }

    def __init__(self, source_path=None, selected_features_list=None,K=None, *args, **kwargs):
        super(KmeansEvaluationJob, self).__init__(*args, **kwargs)
        self.source_path = source_path
        self.selected_features_list = selected_features_list
        self.K = K
        self.check_type()

    def KM(dataset=None, K=6, predict=None):
        # Trains a k-means model.
        kmeans = KMeans().setK(K).setFeaturesCol('features').setPredictionCol('prediction')
        model = kmeans.fit(dataset)

        # Make predictions
        predictions = model.transform(predict)
        predictions.show()

        # Shows the result.
        centers = model.clusterCenters()
        print("Cluster Centers: ")
        for center in centers:
            print(center)
        return centers, predictions

    def evaluation_centers(centers, sample_centers):
        result = []
        for i in range(len(centers)):
            mix = np.array([centers[i], sample_centers[i]])
            result.append(np.corrcoef(mix))
        return result

    def evaluation_prediction(prediction, sample_prediction):
        prediction = prediction.withColumnRenamed('prediction', 'label')
        df = sample_prediction.join(prediction, ['features'])
        df = df.select(['prediction', 'label']).withColumn("label", df.label.cast(DoubleType())).withColumn(
            "prediction", df.prediction.cast(DoubleType()))
        evaluator_accuracy = MulticlassClassificationEvaluator(predictionCol="prediction", metricName="accuracy",
                                                               labelCol='label')
        return evaluator_accuracy.evaluate(df)


    def _statistics(self, df: DataFrame, *args, **kwargs) -> dict:

        source_df = self._get_df_from_source(self.source_path).select(*self.selected_features_list)
        df = df.select(*self.selected_features_list)
        source_df = vectorized_feature(source_df)
        df = vectorized_feature(df)

        if not self.K:
            self.K = random.randint(6, 20)

        centers, predictions = self.KM(source_df, 6, df)
        sample_centers, sample_predictions = self.KM(df, 6, df)

        # 计算中心的相关系数
        centers_result = self.evaluation_centers(centers, sample_centers)
        print(centers_result)

        # 计算抽样中的准确率召回率
        accuracy = self.evaluation_prediction(predictions, sample_predictions)
        print("accuracy: {}".format(accuracy))
        return {"accuracy":accuracy}

