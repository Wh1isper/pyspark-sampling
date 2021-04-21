"""
内部模块常量
"""
# simple sampling
SIMPLE_RANDOM_SAMPLING_METHOD = 'random'
STRATIFIED_SAMPLING_METHOD = 'stratified'

# ml sampling
SMOTE_SAMPLING_METHOD = 'smote'
IMB_ENN_SAMPLING_METHOD = 'imb_enn'

# statistics
STATISTICS_BASIC_METHOD = 'basic'
# evaluation
EVALUATION_COMPARE_METHOD = 'compare'
EVALUATION_DNN_METHOD = 'dnn'
EVALUATION_TESTING_METHOD = 'testing'

FILE_TYPE_TEXT = 'txt'
FILE_TYPE_CSV = 'csv'

# job status in code
JOB_CANCELED = 0
JOB_CREATING = 1
JOB_CREATED = 2

# Job status in db
JOB_STATUS_PADDING = 0
JOB_STATUS_SUCCEED = 200
JOB_STATUS_TYPE_ERROR = 500
JOB_STATUS_KEY_ERROR = 501
JOB_STATUS_PROCESS_ERROR = 599

CODE_TO_SAMPLING_METHOD_NAME = {
    SIMPLE_RANDOM_SAMPLING_METHOD: 'Simple Random Sampling',
    STRATIFIED_SAMPLING_METHOD: 'Stratified Sampling',
    SMOTE_SAMPLING_METHOD: 'Smote Oversampling'
}
CODE_TO_EVALUATION_METHOD_NAME = {
    EVALUATION_COMPARE_METHOD: 'Compare Evaluation',
    EVALUATION_DNN_METHOD: 'DNN Evaluation',
    EVALUATION_TESTING_METHOD: 'Hypothesis Test Evaluation'
}
CODE_TO_JOB_STATUS = {
    JOB_STATUS_PADDING: 'Padding',
    JOB_STATUS_SUCCEED: 'Succeed',
    JOB_STATUS_TYPE_ERROR: 'Error(Type Error)',
    JOB_STATUS_KEY_ERROR: 'Error(Key Error)',
    JOB_STATUS_PROCESS_ERROR: 'Error(Process Error)',
}
