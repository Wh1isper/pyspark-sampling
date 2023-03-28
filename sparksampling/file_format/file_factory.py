from sparksampling.error import BadParamError
from sparksampling.file_format.base_file_format import SparkBaseFileFormat
from sparksampling.file_format.csv_file_imp import CsvFileImpSpark
from sparksampling.mixin import LogMixin
from sparksampling.proto.sampling_service_pb2 import FILE_FORMAT_CSV


class FileFormatFactory(LogMixin):
    format_map = {
        FILE_FORMAT_CSV: CsvFileImpSpark,
    }

    @classmethod
    def _get_imp_class(cls, file_format):
        imp_class = cls.format_map.get(file_format, None)
        if not imp_class:
            raise BadParamError(f"No matching file format: {file_format}")
        return imp_class

    @classmethod
    def _get_format_conf(cls, file_format, format_conf):
        return cls.format_map[file_format].get_init_conf(format_conf)

    @classmethod
    def get_file_imp(cls, spark, file_format, format_conf) -> SparkBaseFileFormat:
        return cls._get_imp_class(file_format)(
            spark, **cls._get_format_conf(file_format, format_conf)
        )
