from typing import Dict

from sparksampling.engine.base_engine import BaseEngine, SparkMixin, record_job_id
from sparksampling.error import CustomErrorWithCode, BadParamError, ProcessError
from sparksampling.file_format.file_factory import FileFormatFactory
from sparksampling.sample import SamplingFactory
from sparksampling.utilities import check_spark_session


class SMSEngine(BaseEngine, SparkMixin):
    # Single Mapping Sampling

    def __init__(self,
                 parent,
                 input_path: str,
                 output_path: str,
                 sampling_method: int,
                 file_format: int,
                 job_id: str,
                 sampling_conf: Dict,
                 format_conf: Dict):
        super(SMSEngine, self).__init__()
        self.parent = parent
        self.input_path = input_path
        self.output_path = output_path
        self.sampling_method = sampling_method
        self.file_format = file_format
        self.job_id = job_id
        self.sampling_conf = sampling_conf
        self.format_conf = format_conf

    @check_spark_session
    def submit(self, *args, **kwargs):
        sampling_imp, file_imp = self.get_spark_imp()
        return self.submit_spark_job(sampling_imp, file_imp)

    def get_spark_imp(self):
        try:
            sampling_imp = SamplingFactory.get_sampling_imp(self.sampling_method, self.sampling_conf)
            file_imp = FileFormatFactory.get_file_imp(self.spark, self.file_format, self.format_conf)
        except CustomErrorWithCode as e:
            raise e
        except KeyError as e:
            self.log.info(f"任务初始化失败 {e}")
            raise BadParamError(f"Key Error, 检查任务配置参数 {str(e)}")
        except ValueError as e:
            self.log.info(f"任务初始化失败 {e}")
            raise BadParamError(f"Value Error, 检查任务配置参数 {str(e)}")
        except Exception as e:
            self.log.exception(e)
            raise ProcessError(str(e))
        return sampling_imp, file_imp

    @record_job_id
    def submit_spark_job(self, sampling_imp, file_imp):
        # 进入spark处理，以下内容抛出错误5000
        df = file_imp.read(self.input_path)
        output_df = sampling_imp.run(df)
        output_path = file_imp.write(output_df, self.output_path)
        self.log.info(f"任务完成，输出文件为: {output_path}")
        return output_path

    @check_spark_session
    def stop(self, job_id=None):
        if not job_id:
            job_id = self.job_id
        self.log.info(f"Send Stop Job to Spark: {job_id}")
        self.spark.sparkContext.cancelJobGroup(job_id)
