from os.path import abspath, dirname, exists, join, split
from pprint import pformat
from typing import Dict
from uuid import uuid4

from sparksampling.engine.base_engine import SparkBaseEngine
from sparksampling.engine_factory import EngineFactory
from sparksampling.error import BadParamError, CustomErrorWithCode, ProcessError
from sparksampling.file_format.file_factory import FileFormatFactory
from sparksampling.mixin import acquire_worker, record_job_id
from sparksampling.proto.sampling_service_pb2 import SamplingRequest
from sparksampling.sample import SamplingFactory
from sparksampling.utilities import check_spark_session


class SMSEngine(SparkBaseEngine):
    # Single Mapping Sampling

    def __init__(
        self,
        parent,
        input_path: str,
        output_path: str,
        sampling_method: int,
        file_format: int,
        job_id: str,
        sampling_conf: Dict or None = None,
        format_conf: Dict or None = None,
    ):
        super(SMSEngine, self).__init__()
        self.parent = parent
        self.input_path = input_path
        self.output_path = output_path
        self.sampling_method = sampling_method
        self.file_format = file_format
        self.job_id = job_id
        self.sampling_conf = sampling_conf
        self.format_conf = format_conf

    @acquire_worker
    @check_spark_session
    def submit(self, *args, **kwargs):
        sampling_imp, file_imp = self.get_spark_imp()
        return self.submit_spark_job(sampling_imp, file_imp)

    def get_spark_imp(self):
        try:
            sampling_imp = SamplingFactory.get_sampling_imp(
                self.sampling_method, self.sampling_conf
            )
            file_imp = FileFormatFactory.get_file_imp(
                self.spark, self.file_format, self.format_conf
            )
        except CustomErrorWithCode as e:
            raise e
        except (KeyError, ValueError) as e:
            self.log.info(f"Task initialization failed {e}")
            raise BadParamError(f"Check task configuration parameters {str(e)}")
        except Exception as e:
            self.log.exception(e)
            raise ProcessError(str(e))
        return sampling_imp, file_imp

    @record_job_id
    def submit_spark_job(self, sampling_imp, file_imp):
        df = file_imp.read(self.input_path)
        df, pre_metas = self.pre_hook(df)
        df = sampling_imp.run(df)
        df, post_metas = self.post_hook(df)
        output_path = file_imp.write(df, self.output_path)
        self.log.info(f"The task is completed and the output file or directory is: {output_path}")
        return output_path, pre_metas + post_metas

    @classmethod
    def config(cls, kwargs):
        def _set_default_output_path(conf):
            if exists(conf.get("input_path")):
                # when input_path = /{path_to_data}/{input_file_name}
                # set default_output_path = /{path_to_data}/sampled/{input_file_name}-{uuid}
                input_path = conf.get("input_path")
                default_output_path = abspath(
                    join(
                        dirname(abspath(input_path)),
                        "./sampled/",
                        split(input_path)[-1] + "-" + str(uuid4()),
                    )
                )
                conf.setdefault("output_path", default_output_path)
            else:
                input_path = conf.get("input_path")
                default_output_path = input_path.rstrip("/") + "-" + str(uuid4())
                conf.setdefault("output_path", default_output_path)
            return conf

        try:
            kwargs = _set_default_output_path(kwargs)
            required_conf = {
                "input_path": kwargs.pop("input_path"),
                "output_path": kwargs.pop("output_path"),
                "sampling_method": kwargs.pop("sampling_method"),
                "file_format": kwargs.pop("file_format"),
                "job_id": kwargs.pop("job_id"),
            }
        except KeyError as e:
            cls.log.info(f"Missing required parameters {str(e)}")
            raise BadParamError(f"Missing required parameters {str(e)}")

        # Detailed configuration depends on the specific implementation
        conf = {
            "sampling_conf": kwargs.get("sampling_conf", {}),
            "format_conf": kwargs.get("format_conf", {}),
        }
        conf.update(required_conf)
        cls.log.info(f"Initializing job conf... \n {pformat(conf)}")
        return conf

    @classmethod
    def is_matching(cls, request_type):
        return request_type == SamplingRequest


EngineFactory.register(SMSEngine)
