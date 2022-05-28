from os.path import abspath, join, exists, dirname, split
from uuid import uuid4

from google.protobuf.json_format import MessageToDict
from pprint import pformat
#
# class EngineFactory(LogMixin):
#     @classmethod
#     def message_to_dict(cls, message):
#         data = MessageToDict(message,
#                              use_integers_for_enums=True,
#                              preserving_proto_field_name=True)
#         return data
from sparksampling.engine.sms_engine import SMSEngine
from sparksampling.error import BadParamError
from sparksampling.mixin import LogMixin


class EngineFactory(LogMixin):
    engine_cls = SMSEngine

    @classmethod
    def __config_engine(cls, kw):
        def __set_default_output_path(kw):
            if exists(kw.get('input_path')):
                # 本地文件情况
                # input_path = /{path_to_data}/{input_file_name}
                # default_output_path = /{path_to_data}/sampled/{input_file_name}-{uuid}
                input_path = kw.get('input_path')
                default_output_path = abspath(
                    join(dirname(abspath(input_path)), "./sampled/", split(input_path)[-1] + '-' + str(uuid4())))
                kw.setdefault('output_path', default_output_path)
            else:
                input_path = kw.get('input_path')
                default_output_path = input_path.rstrip('/') + '-' + str(uuid4())
                kw.setdefault('output_path', default_output_path)
            return kw

        try:
            kw = __set_default_output_path(kw)
            required_conf = {
                'input_path': kw.pop('input_path'),
                'output_path': kw.pop('output_path'),
                'sampling_method': kw.pop('sampling_method'),
                'file_format': kw.pop('file_format'),
                'job_id': kw.pop('job_id')
            }
        except KeyError as e:
            cls.log.info("接口调用错误，必需参数缺失", e)
            raise BadParamError(f"Key Error, 检查服务接口参数 {str(e)}")

        # 非必需参数初始化
        conf = {
            'sampling_conf': kw.get('sampling_conf', {}),
            'format_conf': kw.get('format_conf', {}),
        }
        conf.update(required_conf)
        cls.log.info(f"Init sampling job conf \n {pformat(conf)}")
        return conf

    @classmethod
    def get_engine(cls, parent, **kwargs):
        cls.log.info(f"Using {cls.engine_cls.__name__}")
        return cls.engine_cls(parent, **cls.__config_engine(kwargs))

    @classmethod
    def message_to_dict(cls, message):
        data = MessageToDict(message,
                             use_integers_for_enums=True,
                             preserving_proto_field_name=True)
        return data

    @classmethod
    def cancel_job(cls, parent, job_id):
        # 目前只有一种engine，有增加需求后再设计
        empty_engine = cls.engine_cls(parent, '', '', 0, 0, '', {}, {})
        return empty_engine.stop(job_id)
