import os
import socket

from kubernetes.config.incluster_config import SERVICE_TOKEN_FILENAME
from pyspark import SparkConf
from pyspark.sql import SparkSession

DEPLOY_ON_K8S = os.environ.get('DEPLOY_ON_K8S', 'false') in ['true', 'True']
INCLUSTER = os.path.exists('/var/run/secrets/kubernetes.io/serviceaccount/namespace')


def get_namespace():
    if not INCLUSTER:
        return 'spark-sampling'
    if os.getenv('SPARK_EXECUTOR_NS'):
        return os.getenv('SPARK_EXECUTOR_NS')
    with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace') as f:
        default_namespace = f.read()
    return default_namespace


def get_k8s_config():
    from kubernetes import config
    from kubernetes.client import Configuration

    if INCLUSTER:
        config.load_incluster_config()
    else:
        config.load_kube_config('/root/.kube/config')
    k8s_config = Configuration.get_default_copy()
    token = k8s_config.api_key.get('authorization')
    host = k8s_config.host
    ca = k8s_config.ssl_ca_cert
    key_file = k8s_config.key_file
    cert_file = k8s_config.cert_file

    return host, token, ca, key_file, cert_file


def get_host_ip():
    # only use for minikube debug
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


def set_if_not_none(config_dict, k, v):
    if v is not None:
        config_dict[k] = v


def get_platform_config():
    if DEPLOY_ON_K8S:
        url, token, ca, key_file, cert_file = get_k8s_config()
        master = f"k8s://{url}"
        image = os.getenv('SPARK_EXECUTOR_IMAGE', 'wh1isper/pyspark-executor:latest')

        label_names = os.getenv('SPARK_EXECUTOR_LABELS', '').split(',')
        label_names = (label for label in label_names if label)

        annotation_names = os.getenv('SPARK_EXECUTOR_ANNOTATIONS', '').split(',')
        annotation_names = (annotation for annotation in annotation_names if annotation)

        EXECUTOR_CONFIG = {
            **{f'spark.kubernetes.executor.label.{label_name}': 'true' for label_name in label_names},
            **{f'spark.kubernetes.executor.annotation.{annotation}': 'true' for annotation in annotation_names},
            "spark.executor.instances": os.getenv('SPARK_EXECUTOR_NUMS', '3'),
        }

        SCHEDULER_CONFIG = {
            "spark.master": master,
            "spark.kubernetes.authenticate.caCertFile": ca,
            "spark.kubernetes.authenticate.clientKeyFile": key_file,
            "spark.kubernetes.authenticate.clientCertFile": cert_file,
            "spark.kubernetes.container.image": image,
            "spark.kubernetes.container.image.pullPolicy": 'IfNotPresent',
            **EXECUTOR_CONFIG,
        }

        SCHEDULER_CONFIG = {k: v for k, v in SCHEDULER_CONFIG.items() if v is not None}
        if not INCLUSTER:
            set_if_not_none(SCHEDULER_CONFIG, 'spark.driver.host', get_host_ip())
        else:
            SCHEDULER_CONFIG['spark.kubernetes.authenticate.oauthTokenFile'] = SERVICE_TOKEN_FILENAME
            SCHEDULER_CONFIG['spark.driver.host'] = os.getenv('SPARK_DRIVER_HOST', 'spark-sampling-headless-service')
            SCHEDULER_CONFIG['spark.driver.bindAddress'] = os.getenv('SPARK_DRIVER_BINDADDRESS', '0.0.0.0')
            SCHEDULER_CONFIG['spark.kubernetes.driver.pod.name'] = os.getenv('SPARK_DRIVER_POD_NAME')

        set_if_not_none(SCHEDULER_CONFIG, 'spark.kubernetes.namespace', get_namespace())
    else:
        # as standalone
        SCHEDULER_CONFIG = {
            "spark.master": os.environ.get('SPARK_MASTER', 'local[*]'),
            "spark.driver.memory": os.environ.get('SPARK_DRIVER_MEMORY', '512m'),
        }
    return SCHEDULER_CONFIG


def get_s3_config():
    AK = os.environ.get('S3_ACCESS_KEY', '')
    SK = os.environ.get('S3_SECRET_KEY', '')
    END_POINT = os.environ.get('S3_ENTRY_POINT', '')
    if not (AK and SK and END_POINT):
        return dict()
    return {
        'spark.hadoop.fs.s3a.access.key': AK,
        'spark.hadoop.fs.s3a.secret.key': SK,
        'spark.hadoop.fs.s3a.endpoint': END_POINT,
        "spark.hadoop.fs.s3a.path.style.access": os.environ.get('S3_PATH_STYLE_ACCESS', 'true'),

        # write s3 improvement
        'spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled': 'true',
    }


def get_nfs_mount_config():
    volume_type = 'nfs'
    volume_name = 'driver-nfs-mount'
    mount_path = os.getenv('SPARK_EXECUTOR_VOLUNM_NFS_MOUNT_PATH')
    remote_server = os.getenv('SPARK_EXECUTOR_VOLUNM_NFS_REMOTE_SERVER')
    remote_path = os.getenv('SPARK_EXECUTOR_VOLUNM_NFS_REMOTE_PATH')
    if not (mount_path and remote_path and remote_server):
        return dict()

    return {
        f'spark.kubernetes.driver.volumes.{volume_type}.{volume_name}.mount.path': mount_path,
        f'spark.kubernetes.driver.volumes.{volume_type}.{volume_name}.mount.readOnly': 'false',
        f'spark.kubernetes.driver.volumes.{volume_type}.{volume_name}.options.server': remote_path,
        f'spark.kubernetes.driver.volumes.{volume_type}.{volume_name}.options.path': remote_path,
    }


def get_pvc_mount_config():
    pvc_name = os.getenv('SPARK_EXECUTOR_VOLUNM_PVC_NAME')
    pvc_path = os.getenv('SPARK_EXECUTOR_VOLUNM_PVC_PATH')
    if not (pvc_name and pvc_path):
        return dict()
    return {
        f'spark.kubernetes.executor.volumes.persistentVolumeClaim.{pvc_name}.mount.path': pvc_path,
        f'spark.kubernetes.executor.volumes.persistentVolumeClaim.{pvc_name}.mount.readOnly': 'false',
    }


def get_mount_config():
    return {
        **get_nfs_mount_config(),
        **get_pvc_mount_config(),
    }


def get_spark_conf():
    all_config = {
        "spark.submit.deployMode": 'client',
        **get_platform_config(),
        "spark.app.name": os.environ.get('SPARK_APP_NAME', 'Spark Sampling APP'),
        "spark.ui.port": os.environ.get('SPARK_UI_PORT', '12344'),
        **get_s3_config(),
        # scheduler as FAIR
        "spark.scheduler.mode": "FAIR",
    }

    config = []
    for k, v in all_config.items():
        config.append((k, v))

    spark_config = SparkConf().setAll(config)
    return spark_config


SPARK_CONF = get_spark_conf()


def get_spark():
    return SparkSession.builder.config(conf=SPARK_CONF).getOrCreate()


if __name__ == '__main__':
    spark = get_spark()
