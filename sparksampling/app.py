import findspark

findspark.init()

import os
from concurrent import futures
from pyspark import SparkConf
from pyspark.sql import SparkSession
import logging
import grpc
from sparksampling.config import SPARK_CONF
from sparksampling.service import GRPCService
from traitlets.config import Application
from traitlets import (
    Integer,
    Unicode,
    Dict,
    default,
    Instance
)

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

aliases = {
    'log-level': 'Application.log_level',
    'ip': 'SparkSamplingAPP.ip',
    'port': 'SparkSamplingAPP.port',
    'workers': 'SparkSamplingAPP.workers',
}

flags = {
    'debug': (
        {'Application': {'log_level': logging.DEBUG}},
        "set log level to logging.DEBUG (maximize logging output)",
    ),
}


class SparkSamplingAPP(Application):
    name = 'sparksampling'
    version = "0.1.0"
    description = """An application for starting a spark sampling server"""
    # the grpc server handle
    server = None

    # application config
    aliases = Dict(aliases)
    flags = Dict(flags)

    ip = Unicode(
        '0.0.0.0', help="Host IP address for listening (default 0.0.0.0)."
    ).tag(config=True)

    port = Integer(
        8530, help="Port (default 8530)."
    ).tag(config=True)

    workers = Integer(
        int(os.environ.get('MAX_SAMPLING_WORKERS', 10)), help="Max workers."
    ).tag(config=True)

    spark_config_json = Unicode(
        os.environ.get('SAMPLING_SPARK_EXTRA_CONF_PATH', ''), help="spark_config_json"
    ).tag(config=True)

    spark_config = Instance(SparkConf)
    spark = Instance(SparkSession)

    @default('spark_config')
    def _spark_config_default(self):
        return SPARK_CONF

    @default('spark')
    def _spark_default(self):
        return SparkSession.builder.config(conf=self.spark_config).getOrCreate()

    @default('log_level')
    def _log_level_default(self):
        return logging.DEBUG

    @default('log_datefmt')
    def _log_datefmt_default(self):
        """Exclude date from default date format"""
        return "%Y-%m-%d %H:%M:%S"

    @default('log_format')
    def _log_format_default(self):
        """override default log format to include time"""
        return "[%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d] %(message)s"

    def initialize(self, *args, **kwargs):
        super().initialize(*args, **kwargs)
        self.init_logger()
        self.init_spark()

    def init_logger(self):
        logger = logging.getLogger('sparksampling')
        logger.propagate = True
        logger.parent = self.log
        logger.setLevel(self.log.level)

    def init_spark(self):
        self.log.info(f"Started SparkSession, Spark version: {self.spark.version}")

    def start(self, argv=None):
        self.initialize(argv)
        self._add_server()
        self.server.add_insecure_port('%s:%d' % (self.ip, self.port))
        self.server.start()
        self.log.info("Spark Sampling Server Listening On %s:%s..." %
                      (self.ip, self.port))
        self.server.wait_for_termination()

    def _add_server(self):
        cancel_job_worker_reserve = 1
        max_workers = self.workers + cancel_job_worker_reserve
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
        GRPCService.add_to_server(self, self.server, self.workers)

    @classmethod
    def launch(cls, argv=None):
        self = cls.instance()
        self.start(argv)


main = SparkSamplingAPP.launch

if __name__ == '__main__':
    main()
