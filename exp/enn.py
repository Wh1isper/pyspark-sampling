from imblearn.under_sampling import EditedNearestNeighbours
from pyspark.sql import SparkSession

from sparksampling.config import SPARK_CONF

conf = SPARK_CONF
spark = SparkSession.builder.config(conf=conf).getOrCreate()

df = spark.read.csv("ten_million_top1k.csv", header=True).toPandas()
X = df.drop(columns=['# id', 'y'], axis=1)
y = df[['y']]

enn = EditedNearestNeighbours()
x_fit, y_fit = enn.fit_resample(X.values, y.values)
