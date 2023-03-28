import errno
import os
from concurrent.futures import ThreadPoolExecutor as PoolExecutor

import pandas as pd

from sparksampling.error import ProcessError
from sparksampling.mixin import LogMixin


def fsync_dir(dir_path):
    """
    Execute fsync on a directory ensuring it is synced to disk

    :param str dir_path: The directory to sync
    :raise OSError: If fail opening the directory
    """
    dir_fd = os.open(dir_path, os.O_DIRECTORY)
    try:
        os.fsync(dir_fd)
    except OSError as e:
        # On some filesystem doing a fsync on a directory
        # raises an EINVAL error. Ignoring it is usually safe.
        if e.errno != errno.EINVAL:
            raise
    finally:
        os.close(dir_fd)


class LocalAdapterMixin(LogMixin):
    MERGE_CHUNK_SIZE = 0
    executor = PoolExecutor(os.cpu_count() or 1)

    @staticmethod
    def _merge_local_csv(dir_prefix, csv_files: list, sep):
        merge_file_name = csv_files.pop(0)
        merged_csv_path = os.path.join(dir_prefix, merge_file_name)

        def append_to_file(chunk_file):
            chunk_file_path = os.path.join(dir_prefix, chunk_file)
            with pd.read_csv(chunk_file_path, sep=sep, chunksize=100000, dtype="string") as reader:
                for chunk in reader:
                    chunk.to_csv(
                        merged_csv_path,
                        sep=sep,
                        mode="a",
                        index=False,
                    )
            os.remove(chunk_file_path)

        list(map(append_to_file, csv_files))
        return merge_file_name

    @classmethod
    def _merge_local_csv_parallel(cls, abs_output_path, merge_file_names, sep):
        if len(merge_file_names) == 1:
            return os.path.join(abs_output_path, merge_file_names[0])
        pool = []
        for i in range(0, len(merge_file_names), cls.MERGE_CHUNK_SIZE):
            p = cls.executor.submit(
                cls._merge_local_csv,
                abs_output_path=abs_output_path,
                csv_files=merge_file_names[i : i + cls.MERGE_CHUNK_SIZE],
                sep=sep,
            )
            pool.append(p)

        merge_file_names = [p.result() for p in pool]

        return cls._merge_local_csv_parallel(abs_output_path, merge_file_names, sep)

    @classmethod
    def _merge_local_file(cls, abs_output_path, csv_files, sep):

        if not cls.MERGE_CHUNK_SIZE or cls.MERGE_CHUNK_SIZE == 1:
            return os.path.join(
                abs_output_path,
                cls._merge_local_csv(dir_prefix=abs_output_path, csv_files=csv_files, sep=sep),
            )
        else:
            return cls._merge_local_csv_parallel(abs_output_path, csv_files, sep)

    @classmethod
    def _get_local_sampled_file(cls, output_path, sep):
        abs_dir = os.path.abspath(output_path)
        csv_files = sorted(
            [filename for filename in os.listdir(abs_dir) if filename.endswith(".csv")]
        )
        if not csv_files:
            raise ProcessError("csv file not found")
        if len(csv_files) > 1:
            if os.getenv("NO_REPARTITION"):
                cls.log.info(
                    f"No repartition and no merge for files:  {csv_files[:10]}...(only show top 10)"
                )
                return abs_dir
            cls.log.info(
                f"Start merging sampling files {len(csv_files)}: {csv_files[:10]}...(only show top 10)",
            )
            csv_file = cls._merge_local_file(abs_dir, csv_files, sep)
        else:
            csv_file = csv_files[0]
        cls.log.debug(f"Output one file: {csv_file}")
        fsync_dir(abs_dir)
        return os.path.join(abs_dir, csv_file)


class S3OutputAdapterMixin(LogMixin):
    MERGE_CHUNK_SIZE = 0
    executor = PoolExecutor(os.cpu_count() or 1)
    s3_theme = "s3a://"

    @classmethod
    def _get_s3_sampled_file(cls, output_path, sep, spark):
        def get_s3_conf():
            spark_conf = spark.sparkContext.getConf()
            return {
                "aws_access_key_id": spark_conf.get("spark.hadoop.fs.s3a.access.key"),
                "aws_secret_access_key": spark_conf.get("spark.hadoop.fs.s3a.secret.key"),
                "endpoint_url": spark_conf.get("spark.hadoop.fs.s3a.endpoint"),
            }

        # Experimental function, no need for now
        import boto3

        session = boto3.resource("s3", **get_s3_conf(), verify=False)
        s3_path = output_path.replace(cls.s3_theme, "")
        bucket_name, dir_path = s3_path.split("/", 1)
        bucket = session.Bucket(bucket_name)

        csv_files = sorted(
            [
                object_summary.key
                for object_summary in bucket.objects.filter(Prefix=dir_path)
                if object_summary.key.endswith(".csv")
            ]
        )

        if len(csv_files) > 1:
            cls.log.info(
                f"Generate multiple csv files, s3 does not support automatic merging yet: {csv_files}",
            )
            return output_path
        else:
            csv_file = csv_files[0]
        cls.log.debug(f"Output one file: {csv_file}")
        return cls.s3_theme + "/".join([bucket_name, csv_file])


class OutputAdapterMixin(LocalAdapterMixin, S3OutputAdapterMixin):
    MERGE_CHUNK_SIZE = 0
    executor = PoolExecutor(os.cpu_count() or 1)

    @classmethod
    def _get_sampled_file(cls, output_path, sep, spark=None):
        if output_path.startswith(cls.s3_theme):
            return cls._get_s3_sampled_file(output_path, sep, spark)
        if os.path.exists(output_path):
            return cls._get_local_sampled_file(output_path, sep)
        cls.log.info(
            f"Unsupported file path conversion method, returns the original path: {output_path}"
        )
        return output_path
