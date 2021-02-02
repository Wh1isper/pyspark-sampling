from sparksampling.core.job.base_job import BaseJob


class HypothesisTestEvaluationJob(BaseJob):
    def __init__(self, *args, **kwargs):
        super(HypothesisTestEvaluationJob, self).__init__()
