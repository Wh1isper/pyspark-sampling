from sparksampling.var import STATISTICS_BASIC_METHOD

# compare evaluation方法所对应的statistics method
# 原理见CompareEvaluationJob
# 暂不支持其他方式
compare_evaluation_code = STATISTICS_BASIC_METHOD

# api对应字段：method
# 示例：
# "basic"：BasicStatisticsJob
extra_statistics_job = {

}

# api对应字段：method
# 示例：
# "compare"：CompareEvaluationJob
extra_evaluation_job = {

}

# api对应字段：method
# 示例：
# "random"：SimpleRandomSamplingJob
extra_sampling_job = {

}

# api对应字段：type
# 示例：
# "csv":CsvDataIO
# 可用于所有服务
extra_dataio = {

}
