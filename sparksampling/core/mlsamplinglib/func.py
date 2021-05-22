from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import ArrayType, DoubleType, FloatType
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql import DataFrame


def df_with_column_int(df: DataFrame):
    for c in df.columns:
        df = df.withColumn(c, col(c).cast('int'))
    return df


def df_with_column_float(df: DataFrame):
    for c in df.columns:
        df = df.withColumn(c, col(c).cast(FloatType()))
    return df


def to_array(col):
    def to_array_(v):
        return v.toArray().tolist()

    return udf(to_array_, ArrayType(DoubleType())).asNondeterministic()(col)


def get_num_cat_feat(input_spark_df, exclude_list=None):
    """
    desc: return cat and num features list from a spark df, a step before any encoding on cat features
    inputs:
        * input_spark_df: the input spark dataframe to be checked.
        * exclude_list (list of str): the excluded column name list, which will be excluded for the categorization.
    output:
        * numeric_columns (list of str): the list of numeric column names.
        * string_columns (list of str): the list of categorical column names.
    """
    if exclude_list is None:
        exclude_list = []
    timestamp_columns = [item[0] for item in input_spark_df.dtypes if item[1].lower().startswith(('time', 'date'))]

    # categorize the remaining columns into categorical and numeric columns
    string_columns = [item[0] for item in input_spark_df.dtypes if item[1].lower().startswith('string') \
                      and item[0] not in exclude_list + timestamp_columns]

    numeric_columns = [item[0] for item in input_spark_df.dtypes if
                       item[1].lower().startswith(('big', 'dec', 'doub', 'int', 'float')) \
                       and item[0] not in exclude_list + timestamp_columns]

    # check whether all the columns are covered
    all_cols = timestamp_columns + string_columns + numeric_columns + exclude_list

    if len(set(all_cols)) == len(input_spark_df.columns):
        print("All columns are been covered.")
    elif len(set(all_cols)) < len(input_spark_df.columns):
        not_handle_list = list(set(input_spark_df.columns) - set(all_cols))
        print("Not all columns are covered. The columns missed out: {0}".format(not_handle_list))
    else:
        mistake_list = list(set(all_cols) - set(input_spark_df.columns))
        print("The columns been hardcoded wrongly: {0}".format(mistake_list))

    return numeric_columns, string_columns


def vectorized_feature(x: DataFrame) -> DataFrame:
    x = df_with_column_float(x)
    assembler = VectorAssembler(inputCols=x.columns, outputCol='features')
    stages_ = []
    stages_.append(assembler)
    pipeline = Pipeline(stages=stages_)

    vectorized = pipeline.fit(x).transform(x)
    return vectorized
