from sparksampling.core.job.base_job import BaseJob


class DNNStatisticsJob(BaseJob):
    def __init__(self, *args, **kwargs):
        super(DNNStatisticsJob, self).__init__()
