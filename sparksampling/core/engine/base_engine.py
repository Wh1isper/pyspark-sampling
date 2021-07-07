from pyspark.sql import SparkSession

from sparksampling.config import SPARK_CONF
from sparksampling.core import Logger
from sparksampling.core.dataio import CsvDataIO, TextDataIO, ConfigurableDataIO
from sparksampling.utilities.custom_error import JobTypeError, JobProcessError, JobKeyError, BadParamError
from sparksampling.utilities.utilities import get_value_by_require_dict, from_path_import
from sparksampling.var import FILE_TYPE_TEXT, FILE_TYPE_CSV, CONFIGURABLE_FILE_TYPE
from sparksampling.config import CUSTOM_CONFIG_FILE, SPARK_UI_PORT

extra_dataio = from_path_import("extra_dataio", CUSTOM_CONFIG_FILE, "extra_dataio")


class BaseEngine(Logger):
    data_io_map = {
        FILE_TYPE_TEXT: TextDataIO,
        FILE_TYPE_CSV: CsvDataIO,
        CONFIGURABLE_FILE_TYPE: ConfigurableDataIO,

    }
    data_io_map.update(extra_dataio)

    job_map = {}
    conf = SPARK_CONF
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    SPARK_UI_PORT = spark.conf.get('spark.ui.port')

    def __init__(self):
        for k, v in self.data_io_map.items():
            self.data_io_map[k.lower()] = v
            self.logger.info(f"Load data_io {k} : {v.__name__}")
        for k, v in self.job_map.items():
            self.job_map[k.lower()] = v
            self.logger.info(f"Load job {k} : {v.__name__}")

    def submit(self, job_id=None, df_output=True):
        raise NotImplementedError

    def check_map(self, file_type, method):
        return self.data_io_map.get(file_type) and self.job_map.get(method)


class SparkJobEngine(BaseEngine):
    def __init__(self, path: str, method: str, file_type: str, *args, **kwargs):
        super(SparkJobEngine, self).__init__()
        if not self.check_map(file_type, method):
            raise BadParamError(
                f"Sampling method or file type wrong, expected {self.data_io_map} and {self.job_map}")

        self.job_id = None
        self.path = path
        self.method = method.lower()
        data_io_class = self.data_io_map[file_type.lower()]
        sample_job_class = self.job_map[method.lower()]
        self.job_conf = get_value_by_require_dict(sample_job_class.type_map, kwargs)
        self.data_io_conf = get_value_by_require_dict(data_io_class.type_map, kwargs)
        self.data_io = data_io_class(spark=BaseEngine.spark, path=self.path, **self.data_io_conf)
        self.job = sample_job_class(**self.job_conf)

    def prepare(self, *args, **kwargs) -> dict:
        return {}

    def submit(self, job_id=None, df_output=True, *args, **kwargs):
        self.job_id = job_id
        self.logger.info(f"{self.__class__.__name__}: Prepare for job...job_id: {self.job_id}")
        kwargs.update(**self.prepare(*args, **kwargs))
        self.logger.info(f"{self.__class__.__name__}: Submit job to Spark...job_id: {self.job_id}")
        try:
            df = self.data_io.read(self.job_id, *args, **kwargs)
            if df_output:
                sampled_df = self.job.generate(df, self.job_id, *args, **kwargs)
                return self.data_io.write(sampled_df, *args, **kwargs)
            else:
                return self.job.statistics(df, self.job_id, *args, **kwargs)
        except NotImplementedError as e:
            self.logger.error(e)
            raise
        except TypeError as e:
            raise JobTypeError(str(e))
        except KeyError as e:
            raise JobKeyError(str(e))
        except Exception as e:
            self.logger.warning("Spark processing error...Spark cluster may be dead...")
            raise JobProcessError(str(e))
