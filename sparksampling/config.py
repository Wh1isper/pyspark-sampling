import os

from sparglim.config.builder import ConfigBuilder

DEPLOY_ON_K8S = os.environ.get("DEPLOY_ON_K8S", "false") in ["true", "True"]


def get_spark_conf():
    c: ConfigBuilder = ConfigBuilder()
    configs = {
        "spark.ui.port": os.environ.get("SPARK_UI_PORT", "12344"),
    }
    if DEPLOY_ON_K8S:
        c.config_k8s({**configs})
    else:
        c.config_local(
            {
                **configs,
                "spark.driver.memory": os.environ.get("SPARK_DRIVER_MEMORY", "512m"),
            }
        )
    return c.spark_config


SPARK_CONF = get_spark_conf()


def get_spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.config(conf=SPARK_CONF).getOrCreate()


if __name__ == "__main__":
    spark = get_spark()
