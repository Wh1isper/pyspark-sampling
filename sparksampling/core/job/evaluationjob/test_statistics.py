from sparksampling.core.job.base_job import BaseJob


class HypothesisTestStatisticsJob(BaseJob):
    def __init__(self, *args, **kwargs):
        super(HypothesisTestStatisticsJob, self).__init__()
