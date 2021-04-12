# 此文件用于说明如何自定义添加代码，替换custom_config.py文件即可生效
from sparksampling.utilities.var import STATISTICS_BASIC_METHOD
from sparksampling.customize.dummy_job import DummyJob
from sparksampling.customize.dummy_dataio import DummyDataIO

# compare evaluation方法所对应的statistics method
# 原理见CompareEvaluationJob
# 暂不支持其他方式
compare_evaluation_code = STATISTICS_BASIC_METHOD

# api对应字段：method
# 示例：
# "basic"：BasicStatisticsJob
# PORT: EVALUATION_JOB_PORT
extra_statistics_job = {

}

# api对应字段：method
# 示例：
# "compare"：CompareEvaluationJob
# PORT: EVALUATION_JOB_PORT
extra_evaluation_job = {

}

# api对应字段：method
# 示例：
# "random"：SimpleRandomSamplingJob
# PORT: SAMPLING_JOB_PORT
extra_sampling_job = {
    "dummyJob": DummyJob
}

# api对应字段：type
# 示例：
# "csv":CsvDataIO
# 可用于所有服务
extra_dataio = {
    "dummyDatatype": DummyDataIO
}
