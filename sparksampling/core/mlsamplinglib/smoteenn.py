from pyspark.sql import DataFrame, Window
from sparksampling.core.mlsamplinglib import SparkEditedNearestNeighbours as SparkENN
from sparksampling.core.mlsamplinglib import SparkSMOTE


class SparkSMOTEENN(object):
    def __init__(self, smote: SparkSMOTE, enn: SparkENN):
        self.smote = smote
        self.enn = enn

    def fit_resample(self, x: DataFrame, y: DataFrame) -> DataFrame:
        label_index = self.smote.get_label_index(y)
        oversampled_df = self.smote.fit_resample(x, y)

        x_over = oversampled_df.drop(label_index)
        y_over = oversampled_df.select(label_index)

        return self.enn.fit_resample(x_over, y_over)
