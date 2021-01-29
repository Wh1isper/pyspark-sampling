from sparksampling.core.engine.base_engine import SparkJobEngine
from sparksampling.utilities.var import STATISTICS_BASIC_METHOD
from sparksampling.core.job.statisticsjob import BasicStatisticsJob

from sparksampling.customize.custom_config import extra_statistics_job


class StatisticsEngine(SparkJobEngine):
    job_map = {
        STATISTICS_BASIC_METHOD: BasicStatisticsJob,
    }
    job_map.update(extra_statistics_job)