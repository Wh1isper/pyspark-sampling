import random
from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.ml.linalg import VectorUDT
from pyspark.ml.feature import StringIndexer, VectorAssembler, BucketedRandomProjectionLSH
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from sparksampling.core.mlsamplinglib.func import to_array


def pre_smote_df_process(df: DataFrame, num_cols, cat_cols, target_col, require_indexing=True, index_suffix="_index"):
    '''
    string indexer (optional) and vector assembler
    inputs:
    * df: spark df, original
    * num_cols: numerical cols to be assembled
    * cat_cols: categorical cols to be stringindexed
    * target_col: prediction target
    * index_suffix: will be the suffix after string indexing
    output:
    * vectorized: spark df, after stringindex and vector assemble, ready for smote
    '''
    if (df.select(target_col).distinct().count() != 2):
        raise ValueError("Target col must have exactly 2 classes")

    if target_col in num_cols:
        num_cols.remove(target_col)

    # only assembled numeric columns into features
    assembler = VectorAssembler(inputCols=num_cols, outputCol='features')

    stages_ = []
    stages_.append(assembler)

    # setting to drop original num cols and cat cols
    drop_cols = num_cols

    # index the string cols, except possibly for the label col
    if require_indexing == True:
        str_ind_stages = [StringIndexer(inputCol=column, outputCol=column + index_suffix).fit(df) for column in
                          list(set(cat_cols) - {target_col})]
        stages_ += str_ind_stages
        # also drop cat cols if str index applied
        drop_cols += (cat_cols)

    # add the stage of numerical vector assembler
    pipeline = Pipeline(stages=stages_)

    pos_vectorized = pipeline.fit(df).transform(df)

    keep_cols = [a for a in pos_vectorized.columns if a not in drop_cols]

    vectorized = pos_vectorized.select(*keep_cols).withColumn('label', pos_vectorized[target_col]).drop(target_col)

    print("return num cols vectorized df and stages for testset transformation")

    return vectorized, stages_


def smote(vectorized_sdf, smote_config):
    '''
    contains logic to perform smote oversampling, given a spark df with 2 classes
    inputs:
    * vectorized_sdf: cat cols are already stringindexed, num cols are assembled into 'features' vector
      df target col should be 'label'
    * smote_config: config obj containing smote parameters
    output:
    * oversampled_df: spark df after smote oversampling
    '''
    dataInput_min = vectorized_sdf[vectorized_sdf['label'] == 1]
    dataInput_maj = vectorized_sdf[vectorized_sdf['label'] == 0]

    # LSH, bucketed random projection
    brp = BucketedRandomProjectionLSH(inputCol="features", outputCol="hashes", seed=smote_config.seed,
                                      bucketLength=smote_config.bucket_length)
    # smote only applies on existing minority instances
    model = brp.fit(dataInput_min)
    model.transform(dataInput_min)

    # here distance is calculated from brp's param inputCol
    self_join_w_distance = model.approxSimilarityJoin(dataInput_min, dataInput_min, float("inf"),
                                                      distCol="EuclideanDistance")

    # remove self-comparison (distance 0)
    self_join_w_distance = self_join_w_distance.filter(self_join_w_distance.EuclideanDistance > 0)

    over_original_rows = Window.partitionBy("datasetA").orderBy("EuclideanDistance")

    self_similarity_df = self_join_w_distance.withColumn("r_num", F.row_number().over(over_original_rows))

    self_similarity_df_selected = self_similarity_df.filter(self_similarity_df.r_num <= smote_config.k)

    over_original_rows_no_order = Window.partitionBy('datasetA')

    # list to store batches of synthetic data
    res = []

    # two udf for vector add and subtract, subtraction include a random factor [0,1]
    subtract_vector_udf = F.udf(lambda arr: random.uniform(0, 1) * (arr[0] - arr[1]), VectorUDT())
    add_vector_udf = F.udf(lambda arr: arr[0] + arr[1], VectorUDT())

    # retain original columns
    original_cols = dataInput_min.columns

    for i in range(smote_config.multiplier):
        print("generating batch %s of synthetic instances" % i)
        # logic to randomly select neighbour: pick the largest random number generated row as the neighbour
        df_random_sel = self_similarity_df_selected.withColumn("rand", F.rand()).withColumn('max_rand',
                                                                                            F.max('rand').over(
                                                                                                over_original_rows_no_order)) \
            .where(F.col('rand') == F.col('max_rand')).drop(*['max_rand', 'rand', 'r_num'])
        # create synthetic feature numerical part
        df_vec_diff = df_random_sel.select('*',
                                           subtract_vector_udf(F.array('datasetA.features', 'datasetB.features')).alias(
                                               'vec_diff'))
        df_vec_modified = df_vec_diff.select('*',
                                             add_vector_udf(F.array('datasetA.features', 'vec_diff')).alias('features'))

        # for categorical cols, either pick original or the neighbour's cat values
        for c in original_cols:
            # randomly select neighbour or original data
            col_sub = random.choice(['datasetA', 'datasetB'])
            val = "{0}.{1}".format(col_sub, c)
            if c != 'features':
                # do not unpack original numerical features
                df_vec_modified = df_vec_modified.withColumn(c, F.col(val))

        # this df_vec_modified is the synthetic minority instances,
        df_vec_modified = df_vec_modified.drop(*['datasetA', 'datasetB', 'vec_diff', 'EuclideanDistance'])

        res.append(df_vec_modified)

    dfunion = reduce(DataFrame.unionAll, res)
    # union synthetic instances with original full (both minority and majority) df
    oversampled_df = dfunion.union(vectorized_sdf.select(dfunion.columns))

    return oversampled_df


def restore_smoted_df(num_cols, smoted_df, vectorized_col):
    '''
    restore smoted df to original type
    with original num_cols names
    and stringIndexed cat cols, suffix _index
    depending on to_array udf to unpack vectorized col
    * vectorized_col: str, col that is vectorized
    '''
    # based on the assumption that vectorization is by the list sequence of num_cols
    # to array first
    smoted_df = smoted_df.withColumn("array_num_cols", to_array(col(vectorized_col)))
    # restore all num_cols
    for i in range(len(num_cols)):
        smoted_df = smoted_df.withColumn(num_cols[i], col("array_num_cols")[i])

    drop_cols = [vectorized_col, 'array_num_cols']
    return smoted_df.drop(*drop_cols)
