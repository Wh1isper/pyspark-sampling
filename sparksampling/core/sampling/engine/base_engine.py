from pyspark.sql import SparkSession

from sparksampling.config import SPARK_CONF
from sparksampling.core import Logger
from sparksampling.core.dataio import CsvDataIO, TextDataIO
from sparksampling.utilities.custom_error import JobTypeError, JobProcessError, JobKeyError, BadParamError
from sparksampling.utilities.utilities import extract_none_in_dict, get_value_by_require_dict
from sparksampling.utilities.var import FILE_TYPE_TEXT, FILE_TYPE_CSV

from sparksampling.customize.custom_config import extra_dataio


class BaseEngine(Logger):
    data_io_map = {
        FILE_TYPE_TEXT: TextDataIO,
        FILE_TYPE_CSV: CsvDataIO
    }
    data_io_map.update(extra_dataio)

    job_map = {}
    conf = SPARK_CONF
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    def __init__(self):
        for k, v in self.data_io_map.items():
            self.logger.info(f"Load data_io {k} : {v.__name__}")
        for k, v in self.job_map.items():
            self.logger.info(f"Load job {k} : {v.__name__}")

    def submit(self, job_id):
        raise NotImplementedError

    def check_map(self, file_type, method):
        return self.data_io_map.get(file_type) and self.job_map.get(method)


class SparkJobEngine(BaseEngine):
    def __init__(self, path, method, file_type, *args, **kwargs):
        super(SparkJobEngine, self).__init__()
        if not self.check_map(file_type, method):
            raise BadParamError(
                f"Sampling method or file type wrong, expected {self.data_io_map} and {self.job_map}")

        self.job_id = None
        self.path = path
        self.method = method
        self.data_io_conf = {
            'with_header': kwargs.get('with_header')
        }
        extract_none_in_dict(self.data_io_conf)

        sample_job_class = self.job_map[method]
        self.sampling_job_conf = get_value_by_require_dict(sample_job_class.type_map, kwargs)

        self.data_io = self.data_io_map[file_type](spark=self.spark, path=self.path, **self.data_io_conf)
        self.sample_job = self.job_map[method](**self.sampling_job_conf)

    def submit(self, job_id=None, is_job=True):
        self.job_id = job_id
        self.logger.info(f"{self.__class__.__name__}: Submit job to Spark...job_id: {self.job_id}")
        try:
            df = self.data_io.read(self.job_id)
            if is_job:
                sampled_df = self.sample_job.generate(df, self.job_id)
                return self.data_io.write(sampled_df)
            else:
                return self.sample_job.statistics(df)
        except NotImplementedError as e:
            self.logger.error(e)
            raise
        except TypeError as e:
            raise JobTypeError(str(e))
        except KeyError as e:
            raise JobKeyError(str(e))
        except Exception as e:
            raise JobProcessError(str(e))
