"""
app所需要的环境变量
"""
import os
import json
from pyspark.conf import SparkConf
import random
import string

dir_pre_fix = os.path.abspath(os.path.dirname(__file__))

SPARK_UI_PORT = os.environ.get('SAMPLING_SPARK_UI_PORT', '12344')


def get_spark_conf():
    spark_master = os.environ.get('SAMPLING_SPARK_MASTER', f'local[*]')
    spark_app_name = os.environ.get('SAMPLING_SPARK_APP_NAME', 'Spark Sampling')
    spark_web_ui_port = SPARK_UI_PORT
    ran_str = ''.join(random.sample(string.ascii_letters + string.digits, 8))
    spark_app_name = spark_app_name + ran_str
    spark_extra_conf_path = os.environ.get('SAMPLING_SPARK_EXTRA_CONF_PATH',
                                           os.path.join(dir_pre_fix, 'spark_config.json'))
    spark_extra_conf = []
    with open(spark_extra_conf_path) as f:
        json_config = json.load(f)
    for k, v in json_config.items():
        spark_extra_conf.append((k, v))

    spark_config = SparkConf().setAppName(spark_app_name).setMaster(spark_master).set("spark.ui.port",
                                                                                      spark_web_ui_port).setAll(
        spark_extra_conf)
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
