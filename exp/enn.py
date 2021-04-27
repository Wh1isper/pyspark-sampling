import pandas as pd
from imblearn.under_sampling import EditedNearestNeighbours
from pyspark.sql import SparkSession
from sparksampling.config import SPARK_CONF
from sparksampling.core.mlsamplinglib.enn import SparkEditedNearestNeighbours

conf = SPARK_CONF
spark = SparkSession.builder.config(conf=conf).getOrCreate()
df = spark.read.csv("hdfs://localhost:9000/dataset/ten_million_top1k.csv", header=True)

df = df.limit(100)
y = df.select('y')
drop_list = ['# id', 'y']
x = df.drop(*drop_list)

# x = x.toPandas()
# y = y.toPandas()
# enn = EditedNearestNeighbours(n_neighbors=3)
# x_fit, y_fit = enn.fit_resample(x.values, y.values)

enn = SparkEditedNearestNeighbours(n_neighbors=3)
result_df = enn.fit_resample(x,y)

# result_df = pd.concat([pd.DataFrame(x_fit, columns=x.columns), pd.DataFrame(y_fit, columns=y.columns)], axis=1)
print(result_df.count())
