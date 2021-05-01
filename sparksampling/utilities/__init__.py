from sparksampling.utilities.custom_error import SQLError, DBConnectError, TypeCheckError, JobTypeError, JobKeyError, \
    JobProcessError, JsonDecodeError, BadParamError, CustomErrorWithCode
from sparksampling.utilities.utilities import from_path_import
import logging.config
import os

dir_fix = os.path.abspath(os.path.dirname(__file__))
if not os.path.exists('log'):
    print("Create log dir for log file.")
    os.mkdir('log')
logging.config.fileConfig(os.path.join(dir_fix, "logging.conf"))
logger = logging.getLogger('SAMPLING')
logging.getLogger('pyspark').setLevel(logging.ERROR)
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("matplotlib").setLevel(logging.ERROR)
__all__ = [
    'CustomErrorWithCode',
    'logger',
    'from_path_import',
    'SQLError',
    'JsonDecodeError',
    'BadParamError',
    'DBConnectError',
    'JobKeyError',
    'JobProcessError',
    'JobTypeError',
    'TypeCheckError',
]
