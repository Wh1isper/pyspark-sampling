import os

from sparksampling.file_format.base_file_format import SparkBaseFileFormat
from sparksampling.file_format.output_adapter import OutputAdapterMixin


class CsvFileImpSpark(SparkBaseFileFormat, OutputAdapterMixin):
    cls_args = ["with_header", "sep", "multi_line"]

    def __init__(self, spark, *args, **kwargs):
        super(CsvFileImpSpark, self).__init__(spark, *args, **kwargs)
        self.with_header = kwargs.pop("with_header", False)
        default_sep = "\001" if not os.getenv("COMMA_SEP") else ","
        self.sep = kwargs.pop("sep", default_sep)
        self.multi_line = os.getenv("ENABLE_MULTI_LINE", "True") in ["True", "true"]

    def read(self, input_path):
        return self.spark.read.csv(
            input_path, sep=self.sep, header=self.with_header, multiLine=self.multi_line
        )

    def write(self, df, output_path, output_col=None):
        if output_col:
            self.log.info(f"Write to the specified columns: {output_col}")
            df = df[output_col]
        # When repartition(1), spark will write to a single file
        # Usually this is better for other applications, but there is a performance penalty
        if os.getenv("NO_REPARTITION"):
            df.write.csv(
                output_path,
                sep=self.sep,
                header=self.with_header,
                escapeQuotes=False,
                mode="overwrite",
            )
        else:
            df.repartition(1).write.csv(
                output_path,
                sep=self.sep,
                header=self.with_header,
                escapeQuotes=False,
                mode="overwrite",
            )

        return self._get_sampled_file(output_path, sep=self.sep, spark=self.spark)
