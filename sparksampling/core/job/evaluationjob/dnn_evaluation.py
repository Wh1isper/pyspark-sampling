from sparksampling.core.job.base_job import BaseJob


class DNNEvaluationJob(BaseJob):
    def __init__(self, *args, **kwargs):
        super(DNNEvaluationJob, self).__init__()
