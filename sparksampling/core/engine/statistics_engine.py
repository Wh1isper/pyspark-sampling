from sparksampling.core.engine.base_engine import SparkJobEngine
from sparksampling.utilities import from_path_import
from sparksampling.var import STATISTICS_BASIC_METHOD
from sparksampling.core.job.statisticsjob import BasicStatisticsJob
from sparksampling.config import CUSTOM_CONFIG_FILE

extra_statistics_job = from_path_import("extra_statistics_job", CUSTOM_CONFIG_FILE, "extra_statistics_job")


class StatisticsEngine(SparkJobEngine):
    job_map = {
        STATISTICS_BASIC_METHOD: BasicStatisticsJob,
    }
    job_map.update(extra_statistics_job)
