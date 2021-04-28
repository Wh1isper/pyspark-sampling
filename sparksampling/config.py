"""
app所需要的环境变量
"""
import os
import json
from pyspark.conf import SparkConf

dir_pre_fix = os.path.abspath(os.path.dirname(__file__))


def get_spark_conf():
    spark_master = os.environ.get('SAMPLING_SPARK_MASTER', f'local[{min(32, os.cpu_count() + 4)}]')
    spark_app_name = os.environ.get('SAMPLING_SPARK_APP_NAME', 'Spark Sampling')
    spark_extra_conf_path = os.environ.get('SAMPLING_SPARK_EXTRA_CONF_PATH',
                                           os.path.join(dir_pre_fix, 'spark_config.json'))
    spark_extra_conf = []
    with open(spark_extra_conf_path) as f:
        json_config = json.load(f)
    for k, v in json_config.items():
        spark_extra_conf.append((k, v))

    spark_config = SparkConf().setAppName(spark_app_name).setMaster(spark_master).setAll(spark_extra_conf)
    return spark_config


SPARK_CONF = get_spark_conf()
PORT = int(os.environ.get('SAMPLING_SERVICE_PORT', 8000))
PARALLEL = int(os.environ.get('SAMPLING_SERVICE_PARALLEL', 0))
DEBUG = bool(os.environ.get('SAMPLING_SERVICE_DEBUG', True))

DB_USERNAME = os.environ.get('SAMPLING_DB_USERNAME', 'root')
DB_NAME = os.environ.get('SAMPLING_DB_NAME', 'sampling')
DB_HOST = os.environ.get('SAMPLING_DB_HOST', 'localhost')
DB_PASSWORD = os.environ.get('SAMPLING_DB_PASSWORD', 'baobao')

CUSTOM_CONFIG_FILE = os.environ.get("SAMPLING_CUSTOM_CONFIG_PATH",
                                    os.path.join(dir_pre_fix, "customize/custom_config.py"))
