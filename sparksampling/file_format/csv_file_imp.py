import os

from sparksampling.file_format.base_file_format import SparkBaseFileFormat
from sparksampling.file_format.output_adapter import OutputAdapterMixin


class CsvFileImpSpark(SparkBaseFileFormat, OutputAdapterMixin):
    cls_args = ['with_header', 'sep']

    def __init__(self, spark, *args, **kwargs):
        super(CsvFileImpSpark, self).__init__(spark, *args, **kwargs)
        self.with_header = kwargs.pop('with_header', True)
        self.sep = kwargs.pop('sep', '\001')

    def read(self, input_path):
        return self.spark.read.csv(input_path, sep=self.sep, header=self.with_header)

    def write(self, df, output_path):
        # When repartition(1), spark will write to a single file:
        # Usually this is better for other applications, but there is a performance penalty
        if os.getenv('NO_REPARTITION'):
            df.write.csv(output_path, sep=self.sep, header=self.with_header, mode='overwrite')
        else:
            df.repartition(1).write.csv(output_path, sep=self.sep, header=self.with_header, mode='overwrite')

        return self._get_sampled_file(output_path, sep=self.sep, spark=self.spark)
