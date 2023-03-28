import os

import findspark

findspark.init()

import logging
from concurrent import futures

import grpc
from pyspark import SparkConf
from pyspark.sql import SparkSession
from traitlets import Instance, Integer, Unicode, default
from traitlets.config import Application

from sparksampling import __version__
from sparksampling.config import SPARK_CONF
from sparksampling.service import GRPCService


class SparkSamplingAPP(Application):
    name = "sparksampling"
    description = """An application for starting a spark sampling server"""
    # the grpc server handle
    server = None

    ip = Unicode(
        os.getenv("SERVICE_HOST", "0.0.0.0"),
        help="Host IP address for listening (default 0.0.0.0).",
    ).tag(config=True)

    port = Integer(int(os.getenv("SERVICE_PORT", 8530)), help="Port (default 8530).").tag(
        config=True
    )

    spark_config = Instance(SparkConf)
    spark = Instance(SparkSession)

    config_file_path = Unicode(
        os.path.expanduser("~/.ss/config.py"), help="User defined app config path"
    ).tag(config=True)

    @default("spark_config")
    def _spark_config_default(self):
        return SPARK_CONF

    @default("spark")
    def _spark_default(self):
        return SparkSession.builder.config(conf=self.spark_config).getOrCreate()

    @default("log_level")
    def _log_level_default(self):
        return logging.DEBUG

    @default("log_datefmt")
    def _log_datefmt_default(self):
        """Exclude date from default date format"""
        return "%Y-%m-%d %H:%M:%S"

    @default("log_format")
    def _log_format_default(self):
        """override default log format to include time"""
        return "[%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d] %(message)s"

    def load_config(self):
        dir_prefix = os.path.abspath(os.path.dirname(__file__))
        self.load_config_file(os.path.join(dir_prefix, "default_app_config.py"))
        self.log.info(f"Loading user defined config from {self.config_file_path}")
        self.load_config_file(self.config_file_path)

    def initialize(self, *args, **kwargs):
        self.load_config()
        super().initialize(*args, **kwargs)
        self.init_logger()
        self.log.info(f"Current pyspark-sampling version: {__version__}")
        self.init_spark()

    def init_logger(self):
        logger = logging.getLogger("sparksampling")
        logger.propagate = True
        logger.parent = self.log
        logger.setLevel(self.log.level)

    def init_spark(self):
        self.log.info(f"Started SparkSession, Spark version: {self.spark.version}")

    def start(self, argv=None):
        self.initialize(argv)
        self._add_server()
        self.server.add_insecure_port("%s:%d" % (self.ip, self.port))
        self.server.start()
        self.log.info("Spark Sampling Server Listening On %s:%s..." % (self.ip, self.port))
        self.server.wait_for_termination()

    def _add_server(self):
        GRPCService.register_engine(self)
        engine_worker = GRPCService.get_worker_num()
        # need one reserve worker to reject request asap
        max_workers = engine_worker + 1
        self.log.info(
            f"Service allocate {engine_worker} workers for engine and 1 reserve for response"
        )
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
        GRPCService.add_to_server(self, self.server)

    @classmethod
    def launch(cls, argv=None):
        self = cls.instance()
        self.start(argv)


main = SparkSamplingAPP.launch

if __name__ == "__main__":
    main()
