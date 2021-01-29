"""
内部模块常量
"""
# simple sampling
SIMPLE_RANDOM_SAMPLING_METHOD = 1
STRATIFIED_SAMPLING_METHOD = 2

# ml sampling
SMOTE_SAMPLING_METHOD = 3

# statistics
STATISTICS_BASIC_METHOD = 1
# evaluation
EVALUATION_COMPARE_METHOD = 1
EVALUATION_DNN_METHOD = 2
EVALUATION_TESTING_METHOD = 3

FILE_TYPE_TEXT = 1
FILE_TYPE_CSV = 2

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

CODE_TO_METHOD_NAME = {
    SIMPLE_RANDOM_SAMPLING_METHOD: 'Simple Random Sampling',
    STRATIFIED_SAMPLING_METHOD: 'Stratified Sampling',
    SMOTE_SAMPLING_METHOD: 'Smote Oversampling'
}
CODE_TO_JOB_STATUS = {
    JOB_STATUS_PADDING: 'Padding',
    JOB_STATUS_SUCCEED: 'Succeed',
    JOB_STATUS_TYPE_ERROR: 'Error(Type Error)',
    JOB_STATUS_KEY_ERROR: 'Error(Key Error)',
    JOB_STATUS_PROCESS_ERROR: 'Error(Process Error)',
}
