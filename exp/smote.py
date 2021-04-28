import pandas as pd
from imblearn.over_sampling import SMOTE
from pyspark.sql import SparkSession
from sparksampling.config import SPARK_CONF
from sparksampling.core.mlsamplinglib.smote import SparkSMOTE

conf = SPARK_CONF
spark = SparkSession.builder.config(conf=conf).getOrCreate()
df = spark.read.csv("hdfs://hadoop001.wf:8020/dataset/unbalance_500v50_10.csv", header=True)

y = df.select('y')
drop_list = ['# id', 'y']
x = df.drop(*drop_list)

# x = x.toPandas()
# y = y.toPandas()
# smote = SMOTE(k_neighbors=3)
# x_fit, y_fit = smote.fit_resample(x.values, y.values)

smote = SparkSMOTE(k_neighbors=3)
result_df = smote.fit_resample(x, y)

# result_df = pd.concat([pd.DataFrame(x_fit, columns=x.columns), pd.DataFrame(y_fit, columns=y.columns)], axis=1)
print(result_df.count())

rpd = result_df.toPandas()
print("done")
